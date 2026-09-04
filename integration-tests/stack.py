"""Clients and waiting primitives for the live suite.

The fast suite in ``tests/`` proves the contracts in isolation. This package
proves the *seams*: that a request accepted by the gateway becomes a Kafka
message, a Delta version, a feature row, a vector point and an audited answer,
and that the trace and metric trails through all of it are real.

Three rules shape everything here.

**Wait for a signal, never sleep for a duration.** A distributed stack is
eventually consistent by construction — Kafka has to be polled, Airflow has a
scheduler loop, Prometheus scrapes on an interval, the collector batches spans.
Every wait in this module is a predicate with a deadline that reports what it
last saw, so a failure names the boundary that stalled instead of a timeout.

**Query the control plane, not the log.** Evidence comes from the same APIs an
operator would use during an incident: the Kafka protocol, Airflow's REST API,
Prometheus queries, the trace backend's query API. Nothing here greps stdout.

**Write the evidence the demo needs.** Six of the twelve evidence files can only
be produced from outside the application process (``lab28 evidence`` says so
itself). Those come from these tests, as a side effect of asserting on the same
values — evidence and proof stay the same artefact.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import time
import uuid
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

REPO_ROOT = Path(__file__).resolve().parent.parent
COMPOSE_FILE = REPO_ROOT / "compose.yaml"
EVIDENCE_DIR = REPO_ROOT / "evidence"

#: Every HTTP call here talks to localhost; a slow answer is a real symptom.
HTTP_TIMEOUT = httpx.Timeout(15.0)


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


# --------------------------------------------------------------------------
# Waiting
# --------------------------------------------------------------------------


def wait_until(
    description: str,
    probe: Callable[[], Any],
    *,
    timeout: float = 60.0,
    interval: float = 1.0,
) -> Any:
    """Poll ``probe`` until it returns something truthy, then return it.

    An exception from ``probe`` is treated as "not yet" and kept as the failure
    detail: while a dependency is starting, "ConnectError" *is* the status. Only
    the deadline turns it into an assertion failure, and the message carries the
    last thing observed so the report says which boundary never came up.
    """
    deadline = time.monotonic() + timeout
    last = "nothing observed yet"
    while True:
        try:
            result = probe()
            if result:
                return result
            last = f"probe returned {result!r}"
        except Exception as error:
            last = f"{type(error).__name__}: {error}"
        if time.monotonic() >= deadline:
            raise AssertionError(
                f"timed out after {timeout:.0f}s waiting for {description} — last seen: {last}"
            )
        time.sleep(interval)


def new_trace() -> tuple[str, str]:
    """A caller-supplied W3C trace context.

    The suite generates the trace ID itself rather than reading it back from a
    response, because that is the only way to prove *propagation*: the same ID
    has to reappear in the response header, in the Kafka message headers, in the
    Airflow run and in the trace backend.
    """
    trace_id = uuid.uuid4().hex
    span_id = uuid.uuid4().hex[:16]
    return trace_id, f"00-{trace_id}-{span_id}-01"


def run_id() -> str:
    """A short unique suffix so re-running the suite never collides with itself."""
    return uuid.uuid4().hex[:8]


# --------------------------------------------------------------------------
# Failure injection
# --------------------------------------------------------------------------


def compose(*args: str, timeout: float = 180.0) -> subprocess.CompletedProcess[str]:
    command = ["docker", "compose", "-f", str(COMPOSE_FILE), *args]
    try:
        return subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=REPO_ROOT,
        )
    except subprocess.CalledProcessError as error:
        # Docker Desktop's gRPC client can occasionally terminate with SIGTRAP
        # after a container stop/start. Retry that client-side crash once; a
        # service failure still reaches the existing readiness assertions.
        if error.returncode != -signal.SIGTRAP:
            raise
        time.sleep(1)
        return subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=REPO_ROOT,
        )


@contextmanager
def dependency_down(service: str) -> Iterator[None]:
    """Stop one container for the duration of the block, then start it again.

    ``finally`` is load-bearing: a failed assertion inside the block must still
    restore the stack, or one red test cascades into every test after it.
    """
    compose("stop", service)
    try:
        yield
    finally:
        compose("start", service)


# --------------------------------------------------------------------------
# Evidence
# --------------------------------------------------------------------------


def write_evidence(name: str, payload: Any) -> Path:
    """Write one demo evidence file and return its path."""
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    path = EVIDENCE_DIR / name
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8"
    )
    return path


# --------------------------------------------------------------------------
# Airflow
# --------------------------------------------------------------------------


@dataclass
class Airflow:
    """Airflow 3 REST API v2 client, scoped to the one DAG this lab runs."""

    base_url: str
    username: str
    password: str
    dag_id: str
    _token: str | None = field(default=None, init=False, repr=False)

    def _auth(self) -> dict[str, str]:
        if self._token is None:
            response = httpx.post(
                f"{self.base_url}/auth/token",
                json={"username": self.username, "password": self.password},
                timeout=HTTP_TIMEOUT,
            )
            response.raise_for_status()
            self._token = response.json()["access_token"]
        return {"Authorization": f"Bearer {self._token}"}

    def _get(self, path: str, **params: Any) -> dict[str, Any]:
        response = httpx.get(
            f"{self.base_url}{path}", headers=self._auth(), params=params, timeout=HTTP_TIMEOUT
        )
        response.raise_for_status()
        return response.json()  # type: ignore[no-any-return]

    def health(self) -> dict[str, Any]:
        return self._get("/api/v2/monitor/health")

    def trigger(self, *, conf: dict[str, Any] | None = None, note: str = "") -> str:
        """Start a manual run and return its run id.

        ``logical_date: null`` is what Airflow 3 expects for a run that is not
        filling a schedule interval — sending a made-up date would make the run
        look like a backfill in the UI and in the asset event.
        """
        payload: dict[str, Any] = {
            "dag_run_id": f"it-{run_id()}",
            "logical_date": None,
            "conf": conf or {},
        }
        if note:
            payload["note"] = note
        response = httpx.post(
            f"{self.base_url}/api/v2/dags/{self.dag_id}/dagRuns",
            headers=self._auth(),
            json=payload,
            timeout=HTTP_TIMEOUT,
        )
        response.raise_for_status()
        return response.json()["dag_run_id"]  # type: ignore[no-any-return]

    def run(self, dag_run_id: str) -> dict[str, Any]:
        return self._get(f"/api/v2/dags/{self.dag_id}/dagRuns/{dag_run_id}")

    def wait_for_run(self, dag_run_id: str, *, timeout: float = 300.0) -> dict[str, Any]:
        """Block until the run reaches a terminal state and return it."""

        def terminal() -> dict[str, Any] | None:
            run = self.run(dag_run_id)
            return run if run.get("state") in {"success", "failed"} else None

        return wait_until(  # type: ignore[no-any-return]
            f"Airflow run {dag_run_id} to finish", terminal, timeout=timeout, interval=3.0
        )

    def task_instances(self, dag_run_id: str) -> list[dict[str, Any]]:
        body = self._get(f"/api/v2/dags/{self.dag_id}/dagRuns/{dag_run_id}/taskInstances")
        return list(body.get("task_instances", []))

    def asset_events(self, dag_run_id: str) -> list[dict[str, Any]]:
        """Asset events produced by one run — the IP02 output contract."""
        body = self._get(
            "/api/v2/assets/events", source_dag_id=self.dag_id, source_run_id=dag_run_id
        )
        return list(body.get("asset_events", []))


# --------------------------------------------------------------------------
# Prometheus
# --------------------------------------------------------------------------


@dataclass
class Prometheus:
    base_url: str

    def query(self, expression: str) -> list[dict[str, Any]]:
        response = httpx.get(
            f"{self.base_url}/api/v1/query", params={"query": expression}, timeout=HTTP_TIMEOUT
        )
        response.raise_for_status()
        body = response.json()
        assert body["status"] == "success", f"Prometheus rejected {expression!r}: {body}"
        return list(body["data"]["result"])

    def value(self, expression: str) -> float | None:
        """The single scalar for ``expression``, or ``None`` when it has no data.

        ``None`` and ``0.0`` are different answers and the tests branch on it: a
        counter that has never been incremented does not exist yet, which is not
        the same as a counter sitting at zero.
        """
        results = self.query(expression)
        if not results:
            return None
        return float(results[0]["value"][1])

    def targets(self) -> list[dict[str, Any]]:
        response = httpx.get(f"{self.base_url}/api/v1/targets", timeout=HTTP_TIMEOUT)
        response.raise_for_status()
        return list(response.json()["data"]["activeTargets"])

    def rules(self) -> list[dict[str, Any]]:
        response = httpx.get(f"{self.base_url}/api/v1/rules", timeout=HTTP_TIMEOUT)
        response.raise_for_status()
        groups = response.json()["data"]["groups"]
        return [rule for group in groups for rule in group.get("rules", [])]


# --------------------------------------------------------------------------
# Trace backend
# --------------------------------------------------------------------------


@dataclass
class TraceBackend:
    """Jaeger query API — the local, deterministic half of IP10.

    LangSmith is the other half and is gated on a credential; the local backend
    is what makes the trace assertions runnable offline and in CI.
    """

    base_url: str

    def trace(self, trace_id: str) -> dict[str, Any] | None:
        response = httpx.get(f"{self.base_url}/api/traces/{trace_id}", timeout=HTTP_TIMEOUT)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        data = response.json().get("data") or []
        return dict(data[0]) if data else None

    def spans(self, trace_id: str) -> list[dict[str, Any]]:
        trace = self.trace(trace_id)
        return list(trace.get("spans", [])) if trace else []

    def span_names(self, trace_id: str) -> set[str]:
        return {span["operationName"] for span in self.spans(trace_id)}

    def service_names(self, trace_id: str) -> set[str]:
        trace = self.trace(trace_id)
        if not trace:
            return set()
        return {
            process.get("serviceName", "")
            for process in (trace.get("processes") or {}).values()
        }

    def wait_for_spans(
        self, trace_id: str, expected: Sequence[str], *, timeout: float = 120.0
    ) -> set[str]:
        """Wait until every name in ``expected`` is present in the trace."""
        wanted = set(expected)

        def complete() -> set[str] | None:
            seen = self.span_names(trace_id)
            return seen if wanted <= seen else None

        try:
            return wait_until(  # type: ignore[no-any-return]
                f"spans {sorted(wanted)} on trace {trace_id}",
                complete,
                timeout=timeout,
                interval=2.0,
            )
        except AssertionError as error:
            seen = self.span_names(trace_id)
            raise AssertionError(
                f"{error}\nmissing span names: {sorted(wanted - seen)}\npresent: {sorted(seen)}"
            ) from None


# --------------------------------------------------------------------------
# Kafka
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Record:
    """One consumed message, decoded far enough to assert on."""

    key: str | None
    value: dict[str, Any] | None
    raw: bytes
    headers: dict[str, str]
    partition: int
    offset: int

    @property
    def traceparent(self) -> str | None:
        return self.headers.get("traceparent")

    @property
    def trace_id(self) -> str | None:
        """The trace ID field of the W3C header, if the message carries one."""
        parts = (self.traceparent or "").split("-")
        return parts[1] if len(parts) == 4 else None


def read_topic(
    bootstrap_servers: str,
    topic: str,
    *,
    timeout: float = 20.0,
    limit: int = 1000,
) -> list[Record]:
    """Read a topic from the beginning, without joining a consumer group.

    Explicit partition assignment rather than ``subscribe`` is deliberate: group
    coordination adds a rebalance delay and, worse, makes the read depend on
    committed offsets — a test must see every message on the topic, including
    ones the pipeline has already consumed.
    """
    from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"lab28-it-{run_id()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    records: list[Record] = []
    try:
        metadata = consumer.list_topics(topic, timeout=10.0)
        topic_metadata = metadata.topics.get(topic)
        if topic_metadata is None or topic_metadata.error is not None:
            raise AssertionError(f"topic {topic!r} does not exist on {bootstrap_servers}")
        consumer.assign(
            [
                TopicPartition(topic, partition, OFFSET_BEGINNING)
                for partition in topic_metadata.partitions
            ]
        )

        deadline = time.monotonic() + timeout
        idle = 0
        while len(records) < limit and time.monotonic() < deadline and idle < 3:
            message = consumer.poll(1.0)
            if message is None:
                idle += 1
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    idle += 1
                    continue
                raise AssertionError(f"consume failed: {message.error()}")
            idle = 0
            raw = message.value() or b""
            try:
                value = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                # A poison message is exactly what IT-J4 puts here on purpose.
                value = None
            records.append(
                Record(
                    key=message.key().decode("utf-8") if message.key() else None,
                    value=value,
                    raw=raw,
                    headers={
                        name: (raw_value or b"").decode("utf-8", "replace")
                        for name, raw_value in (message.headers() or [])
                    },
                    partition=message.partition(),
                    offset=message.offset(),
                )
            )
    finally:
        consumer.close()
    return records


def produce_raw(bootstrap_servers: str, topic: str, *, key: str, value: bytes) -> None:
    """Put arbitrary bytes on a topic — the failure injection behind the DLQ test."""
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": bootstrap_servers})
    producer.produce(topic, key=key.encode("utf-8"), value=value)
    remaining = producer.flush(10.0)
    assert remaining == 0, f"could not deliver the poison message to {topic}"


# --------------------------------------------------------------------------
# Qdrant
# --------------------------------------------------------------------------


def qdrant_count(url: str, collection: str, *, doc_id: str | None = None) -> int:
    """Exact point count, optionally filtered to one document.

    ``exact=True`` matters: the approximate count is an estimate from the
    segment metadata and can lag an upsert, which would make an idempotency
    assertion flap.
    """
    body: dict[str, Any] = {"exact": True}
    if doc_id is not None:
        body["filter"] = {"must": [{"key": "doc_id", "match": {"value": doc_id}}]}
    response = httpx.post(
        f"{url.rstrip('/')}/collections/{collection}/points/count",
        json=body,
        timeout=HTTP_TIMEOUT,
    )
    response.raise_for_status()
    return int(response.json()["result"]["count"])


def qdrant_point(url: str, collection: str, point_id: str) -> dict[str, Any] | None:
    response = httpx.get(
        f"{url.rstrip('/')}/collections/{collection}/points/{point_id}", timeout=HTTP_TIMEOUT
    )
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return dict(response.json()["result"])


# --------------------------------------------------------------------------
# Metrics exposition
# --------------------------------------------------------------------------


def scrape(url: str) -> str:
    """Fetch a ``/metrics`` exposition as text."""
    response = httpx.get(url, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    return response.text


def metric_total(exposition: str, prefix: str) -> float:
    """Sum every sample whose series name starts with ``prefix``.

    Summing rather than matching one label set keeps the assertions readable:
    the tests care that a counter moved, not about the exact label combination
    the client happened to use.
    """
    total = 0.0
    for line in exposition.splitlines():
        if line.startswith("#") or not line.startswith(prefix):
            continue
        parts = line.rsplit(" ", 1)
        if len(parts) != 2:
            continue
        try:
            total += float(parts[1])
        except ValueError:
            continue
    return total
