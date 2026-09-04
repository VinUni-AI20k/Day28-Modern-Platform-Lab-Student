"""IT-J4 — what the platform does when a dependency is gone.

Every other journey shows the happy path working. This one takes pieces away
and asserts on the three answers that decide whether an outage becomes an
incident:

*Which failures remove a pod from rotation.* Feast is reported by ``/ready`` but
is not mandatory, so losing it must never turn into a 503 — a 503 takes the pod
out of the gateway's rotation and turns a colder answer into no answer at all.
Qdrant is mandatory, so losing it must produce exactly that 503.

*What a caller gets instead of an error.* A degraded answer is a product
decision, not an accident: it is served, it says so in its evidence, it names
the component that was missing, and it increments a counter an operator can
alert on.

*Whether anything was lost.* A message the pipeline cannot parse is parked with
enough context to diagnose it, the good records in the same batch still land,
and a parked event can be put back on the topic once the defect is fixed.

The absolute-``ready`` assertions carry the ``gpu`` marker rather than a bare
skip: the inference endpoint is a mandatory probe, so on a stack without a real
vLLM the pod is *correctly* not ready and those assertions would be measuring
the missing GPU instead of the failure injection. Everything else is written
against the readiness verdict observed before the injection, so it stays honest
on any stack.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import Any

import httpx
import pytest
import stack
from stack import Airflow

from lab28_platform.contracts import DeadLetterEnvelope, ErrorCategory
from lab28_platform.event_bus import (
    dead_letter_count,
    decode_dead_letters,
    replay_dead_letters,
)
from lab28_platform.settings import Settings

pytestmark = [pytest.mark.integration, pytest.mark.matrix("IT-J4-degraded-recovery")]

#: Compose service names, overridable so the suite survives a renamed service.
FEAST_SERVICE = stack.env("LAB28_COMPOSE_FEAST", "feast")
QDRANT_SERVICE = stack.env("LAB28_COMPOSE_QDRANT", "qdrant")

QUESTION = "Nền tảng dữ liệu của lab này gồm những thành phần nào?"


@dataclass(frozen=True)
class Baseline:
    """The readiness verdict before anything was broken."""

    status_code: int
    status: str


def _readiness(client: httpx.Client) -> tuple[int, dict[str, Any]]:
    response = client.get("/ready")
    return response.status_code, dict(response.json())


def _wait_for_status(
    client: httpx.Client, wanted: str, *, timeout: float = 120.0
) -> tuple[int, dict[str, Any]]:
    """Poll ``/ready`` until it reports ``wanted``, then return code and body."""

    def matching() -> tuple[int, dict[str, Any]] | None:
        observed = _readiness(client)
        return observed if observed[1].get("status") == wanted else None

    return stack.wait_until(  # type: ignore[no-any-return]
        f"/ready to report {wanted!r}", matching, timeout=timeout, interval=2.0
    )


def _component(body: dict[str, Any], name: str) -> dict[str, Any]:
    for entry in body.get("components", []):
        if entry["name"] == name:
            return dict(entry)
    raise AssertionError(f"{name!r} is not in the readiness breakdown: {body}")


def _degraded_total(settings: Settings, reason: str) -> float:
    exposition = stack.scrape(f"{settings.api_url}/metrics")
    return stack.metric_total(exposition, f'lab28_degraded_responses_total{{reason="{reason}"}}')


def _ask(client: httpx.Client, *, asker_id: str) -> dict[str, Any]:
    response = client.post(
        "/api/v1/ask",
        json={"asker_id": asker_id, "question": QUESTION, "top_k": 3},
        timeout=90.0,
    )
    assert response.status_code == 200, response.text
    return dict(response.json())


@pytest.fixture(scope="module")
def baseline(api: httpx.Client) -> Baseline:
    """Read the verdict once, so recovery is asserted against reality.

    Hard-coding ``ready`` here would make every recovery assertion fail on a
    stack that is missing an unrelated mandatory dependency, for a reason that
    has nothing to do with the outage being injected.
    """
    status_code, body = _readiness(api)
    return Baseline(status_code=status_code, status=str(body["status"]))


# -- IP04: a degradable dependency -------------------------------------------


def test_losing_the_feature_store_does_not_change_the_rotation_verdict(
    api: httpx.Client, baseline: Baseline
) -> None:
    """Feast is reported, not mandatory: it colours the answer, it does not gate it."""

    def probe_reports_the_outage() -> tuple[int, dict[str, Any]] | None:
        observed = _readiness(api)
        return observed if not _component(observed[1], "feast")["ready"] else None

    with stack.dependency_down(FEAST_SERVICE):
        status_code, body = stack.wait_until(
            "the feature store probe to report the outage",
            probe_reports_the_outage,
            timeout=120.0,
            interval=2.0,
        )

        assert body["status"] != "not_ready", body
        assert status_code == baseline.status_code, "a degradable outage changed the verdict"
        assert _component(body, "feast")["owner"], "an unready component needs an owner"

    # Restoring the container has to restore the probe, or the outage is permanent.
    stack.wait_until(
        "the feature store probe to recover",
        lambda: _component(_readiness(api)[1], "feast")["ready"],
        timeout=180.0,
        interval=3.0,
    )


@pytest.mark.gpu
def test_the_feature_store_outage_reads_as_degraded_not_broken(api: httpx.Client) -> None:
    """``degraded`` is the whole point of the distinction: served, and honest about it."""
    with stack.dependency_down(FEAST_SERVICE):
        status_code, body = _wait_for_status(api, "degraded")

        assert status_code == 200, "a degradable outage must not eject the pod"
        assert _component(body, "feast")["ready"] is False
        assert _component(body, "qdrant")["ready"] is True

    _wait_for_status(api, "ready", timeout=180.0)


@pytest.mark.gpu
def test_an_answer_is_still_served_without_features(
    settings: Settings, api: httpx.Client
) -> None:
    """No features is a colder answer, not a failed request — and it is counted."""
    before = _degraded_total(settings, "feast")

    with stack.dependency_down(FEAST_SERVICE):
        _wait_for_status(api, "degraded")
        answer = _ask(api, asker_id=f"it-j4-{stack.run_id()}")
        after = _degraded_total(settings, "feast")

    evidence = answer["evidence"]
    reasons = " ".join(evidence["degraded_reasons"]).lower()

    assert answer["answer"].strip(), "a degraded answer is still an answer"
    assert evidence["degraded"] is True
    assert "feature store" in reasons, evidence["degraded_reasons"]
    assert evidence["feature_freshness_seconds"] is None
    assert after > before, "a degraded response nobody can alert on is invisible"

    _wait_for_status(api, "ready", timeout=180.0)


# -- IP05: a mandatory dependency ---------------------------------------------


def test_losing_the_vector_store_fails_the_readiness_check_closed(
    api: httpx.Client, baseline: Baseline
) -> None:
    """Retrieval is what makes an answer grounded, so its absence is a 503."""
    with stack.dependency_down(QDRANT_SERVICE):
        status_code, body = _wait_for_status(api, "not_ready", timeout=120.0)

        assert status_code == 503
        assert _component(body, "qdrant")["ready"] is False
        assert _component(body, "qdrant")["detail"], "a 503 with no reason is unactionable"

    status_code, body = _wait_for_status(api, baseline.status, timeout=180.0)
    assert status_code == baseline.status_code
    assert _component(body, "qdrant")["ready"] is True


@pytest.mark.gpu
def test_the_gateway_stops_routing_to_a_pod_that_is_not_ready(
    api: httpx.Client, gateway: httpx.Client
) -> None:
    """The 503 has to come from the gateway, not from the app behind it.

    The two are told apart by the body: the app answers a readiness failure with
    its own JSON breakdown, while an ejected upstream produces the gateway's own
    error. This is also the test that catches Envoy's default 50 %
    ``healthy_panic_threshold`` — with a single upstream host, ejecting it puts
    the cluster into panic mode and traffic keeps flowing to the unready pod.
    """
    _wait_for_status(api, "ready", timeout=180.0)

    def rejected_by_the_gateway() -> httpx.Response | None:
        response = gateway.get("/ready")
        if response.status_code != 503 or "components" in response.text:
            return None
        return response

    with stack.dependency_down(QDRANT_SERVICE):
        _wait_for_status(api, "not_ready", timeout=120.0)
        rejection = stack.wait_until(
            "the gateway to take the unready pod out of rotation",
            rejected_by_the_gateway,
            timeout=120.0,
            interval=2.0,
        )

        assert rejection.status_code == 503
        # The pod itself is alive the whole time; it is out of rotation, not down.
        direct_code, direct_body = _readiness(api)
        assert direct_code == 503
        assert direct_body["status"] == "not_ready"

    _wait_for_status(api, "ready", timeout=180.0)


@pytest.mark.gpu
def test_a_pod_out_of_rotation_still_answers_a_direct_request(api: httpx.Client) -> None:
    """Not ready means "do not send me traffic", not "I cannot work".

    Readiness and serving make the call independently, and they are allowed to
    disagree: retrieval is mandatory for rotation because an ungrounded answer
    is worth less, and degradable in the request path because a caller who
    reached this pod anyway is better served by a flagged answer than by a 503.
    """
    with stack.dependency_down(QDRANT_SERVICE):
        _wait_for_status(api, "not_ready", timeout=120.0)
        answer = _ask(api, asker_id=f"it-j4-{stack.run_id()}")

    evidence = answer["evidence"]
    reasons = " ".join(evidence["degraded_reasons"]).lower()

    assert evidence["degraded"] is True
    assert "retrieval" in reasons or "vector store" in reasons, evidence["degraded_reasons"]
    assert answer["sources"] == [], "no index, no grounding — an empty source list is the truth"

    _wait_for_status(api, "ready", timeout=180.0)


# -- IP02: a message the pipeline cannot parse --------------------------------


@dataclass
class PoisonBatch:
    asker_id: str
    key: str
    payload: bytes
    dead_letters_before: int
    dead_letters_after: int
    run: dict[str, Any]
    envelopes: list[dict[str, Any]]


@pytest.fixture(scope="module")
def poison_batch(settings: Settings, gateway: httpx.Client, airflow: Airflow) -> PoisonBatch:
    """One good record and one unparseable message, delivered in the same batch."""
    suffix = stack.run_id()
    asker_id = f"it-j4-{suffix}"
    key = f"it-j4-poison-{suffix}"
    payload = f'{{"schema_version": "1", "event_id": "{suffix}", truncated'.encode()

    before = dead_letter_count(settings.kafka)
    stack.wait_until(
        "the gateway to re-admit the recovered API",
        lambda: gateway.get("/ready").status_code == 200,
        timeout=120.0,
        interval=2.0,
    )
    accepted = gateway.post(
        "/api/v1/feedback",
        json={
            "asker_id": asker_id,
            "text": f"Phản hồi hợp lệ đi cùng bản tin hỏng {suffix}.",
            "rating": 5,
            "label": "positive",
        },
    )
    assert accepted.status_code == 202, accepted.text
    stack.produce_raw(
        settings.kafka.bootstrap_servers, settings.kafka.topic_raw, key=key, value=payload
    )

    run = airflow.wait_for_run(airflow.trigger(note=f"IT-J4 poison batch {suffix}"))

    return PoisonBatch(
        asker_id=asker_id,
        key=key,
        payload=payload,
        dead_letters_before=before,
        dead_letters_after=dead_letter_count(settings.kafka),
        run=run,
        envelopes=decode_dead_letters(settings.kafka, limit=100),
    )


def test_one_unparseable_message_does_not_fail_the_batch(poison_batch: PoisonBatch) -> None:
    """A pipeline that dies on malformed input hands the outage to whoever sent it."""
    assert poison_batch.run["state"] == "success", poison_batch.run


def test_the_unparseable_message_is_parked_rather_than_dropped(
    settings: Settings, poison_batch: PoisonBatch
) -> None:
    """Parked with the coordinates to find it again — topic, partition, offset, key."""
    assert poison_batch.dead_letters_after > poison_batch.dead_letters_before

    mine = [
        envelope
        for envelope in poison_batch.envelopes
        if base64.b64decode(envelope["raw_payload_b64"]) == poison_batch.payload
    ]

    assert len(mine) == 1, f"expected one dead letter for the poison message, found {len(mine)}"
    envelope = mine[0]
    assert envelope["original_topic"] == settings.kafka.topic_raw
    assert envelope["original_key"] == poison_batch.key
    assert envelope["original_partition"] >= 0
    assert envelope["original_offset"] >= 0
    assert envelope["error_category"] == ErrorCategory.VALIDATION.value
    assert envelope["error_detail"], "a dead letter with no diagnosis is just a lost message"
    assert envelope["attempts"] >= 1


def test_the_good_record_in_the_same_batch_still_reached_the_lakehouse(
    settings: Settings, poison_batch: PoisonBatch
) -> None:
    """No data loss: the bad message must not take the good ones with it."""
    from lab28_platform import delta_store

    rows = [
        row
        for row in delta_store.read_rows(settings.feedback_table)
        if row.get("asker_id") == poison_batch.asker_id
    ]

    assert len(rows) == 1, rows


# -- IP02: putting a parked event back ----------------------------------------


@dataclass
class Replay:
    asker_id: str
    idempotency_key: str
    result: dict[str, int]
    run: dict[str, Any]


@pytest.fixture(scope="module")
def replayed(
    settings: Settings,
    gateway: httpx.Client,
    airflow: Airflow,
    poison_batch: PoisonBatch,
) -> Replay:
    """Park a *valid* event on the dead-letter topic, then replay the topic.

    Replay exists for the second kind of dead letter: a well-formed event whose
    batch failed downstream. Producing one naturally means breaking the
    lakehouse for every other test in the suite, so the envelope is injected —
    but the payload inside it is not invented, it is the exact bytes the API
    published for a real submission, read back off the topic.

    ``poison_batch`` is requested for its ordering: the unparseable message must
    already be parked, so that one replay call exercises both outcomes.
    """
    suffix = stack.run_id()
    asker_id = f"it-j4-replay-{suffix}"
    accepted = gateway.post(
        "/api/v1/feedback",
        json={
            "asker_id": asker_id,
            "text": f"Phản hồi được phát lại từ hàng đợi lỗi {suffix}.",
            "rating": 3,
            "label": "neutral",
        },
    )
    assert accepted.status_code == 202, accepted.text
    idempotency_key = str(accepted.json()["idempotency_key"])

    def published() -> stack.Record | None:
        records = stack.read_topic(settings.kafka.bootstrap_servers, settings.kafka.topic_raw)
        return next(
            (
                record
                for record in records
                if record.value and record.value.get("idempotency_key") == idempotency_key
            ),
            None,
        )

    record = stack.wait_until(
        f"the event for {asker_id} to reach {settings.kafka.topic_raw}",
        published,
        timeout=60.0,
        interval=3.0,
    )

    envelope = DeadLetterEnvelope(
        original_topic=settings.kafka.topic_raw,
        original_partition=record.partition,
        original_offset=record.offset,
        original_key=record.key,
        error_category=ErrorCategory.INTERNAL,
        error_detail="parked by the live suite to exercise the operator replay path",
        attempts=3,
        traceparent=record.traceparent,
        raw_payload_b64=base64.b64encode(record.raw).decode("ascii"),
    )
    stack.produce_raw(
        settings.kafka.bootstrap_servers,
        settings.kafka.topic_dlq,
        key=asker_id,
        value=envelope.model_dump_json().encode("utf-8"),
    )

    result = replay_dead_letters(settings.kafka, limit=100, group_suffix=f"it-{suffix}")
    run = airflow.wait_for_run(airflow.trigger(note=f"IT-J4 replay {suffix}"))

    return Replay(
        asker_id=asker_id, idempotency_key=idempotency_key, result=result, run=run
    )


def test_replay_puts_a_parked_event_back_on_its_original_topic(
    settings: Settings, replayed: Replay
) -> None:
    """The copy is matched by idempotency key rather than by partition key.

    Replay republishes under the key the envelope was stored with, which is not
    necessarily the key the API used, so the key is not what identifies the
    event here — the content-derived idempotency key is.
    """
    assert replayed.result["replayed"] >= 1, replayed.result

    records = stack.read_topic(settings.kafka.bootstrap_servers, settings.kafka.topic_raw)
    copies = [
        record
        for record in records
        if record.value and record.value.get("idempotency_key") == replayed.idempotency_key
    ]

    assert len(copies) >= 2, f"the replayed copy never reached the topic: {len(copies)} found"


def test_replay_refuses_to_reinject_a_payload_it_cannot_parse(replayed: Replay) -> None:
    """The poison message is skipped, not replayed — otherwise it loops forever."""
    assert replayed.result["skipped"] >= 1, replayed.result


def test_the_replayed_event_does_not_duplicate_the_row(
    settings: Settings, replayed: Replay
) -> None:
    """Recovery is only safe because the write is idempotent: replay, one row."""
    from lab28_platform import delta_store

    assert replayed.run["state"] == "success", replayed.run
    rows = [
        row
        for row in delta_store.read_rows(settings.feedback_table)
        if row.get("asker_id") == replayed.asker_id
    ]

    assert len(rows) == 1, rows
    assert rows[0]["idempotency_key"] == replayed.idempotency_key


# -- recovery -----------------------------------------------------------------


def test_the_platform_ends_where_it_started(api: httpx.Client, baseline: Baseline) -> None:
    """The receipt for every container this module stopped."""
    status_code, body = _wait_for_status(api, baseline.status, timeout=180.0)

    assert status_code == baseline.status_code
    assert _component(body, "feast")["ready"] is True
    assert _component(body, "qdrant")["ready"] is True
