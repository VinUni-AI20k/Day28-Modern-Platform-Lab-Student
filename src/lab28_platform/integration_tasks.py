"""Four student-owned boundaries used by the live platform.

Run ``uv run pytest starter-tests -q`` while completing these functions.  Do
not change their signatures: Kafka, Delta, Feast and ``/ready`` call them.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from lab28_platform.contracts import FEATURE_REFS, IngestionEvent

#: W3C trace context header. Omitted entirely when no trace is active, because
#: an empty value is not a valid ``traceparent`` and would break IP10.
HEADER_TRACEPARENT = "traceparent"

#: Logical de-duplication key carried next to the payload so IP03 can collapse
#: a Kafka replay without re-parsing the message body.
HEADER_IDEMPOTENCY_KEY = "idempotency-key"

#: Feast entity join key for ``asker_activity_v1``
#: (``feature-repo/definitions.py``: ``Entity(name="asker", join_keys=[...])``).
FEAST_ENTITY_JOIN_KEY = "asker_id"


def event_headers(
    traceparent: str | None, idempotency_key: str
) -> list[tuple[str, bytes]]:
    """Return byte-valued Kafka headers for trace and replay correlation.

    ``idempotency-key`` is always required.  Omit ``traceparent`` when no trace
    is active rather than sending an empty, invalid W3C header.
    """
    # A list (not a tuple) because the producer appends ``schema_version``.
    headers = [(HEADER_IDEMPOTENCY_KEY, idempotency_key.encode("utf-8"))]
    if traceparent:
        headers.append((HEADER_TRACEPARENT, traceparent.encode("utf-8")))
    return headers


def dedupe_latest(events: Iterable[IngestionEvent]) -> list[IngestionEvent]:
    """Return one newest event per idempotency key, in deterministic key order.

    Compare ``(occurred_at, event_id)`` so ties do not depend on Kafka delivery
    order.  The Spark Delta MERGE calls this through ``delta_store``.
    """
    winners: dict[str, IngestionEvent] = {}
    # Single pass, so a consumed generator is still handled correctly.
    for event in events:
        incumbent = winners.get(event.idempotency_key)
        if incumbent is None or _merge_rank(event) > _merge_rank(incumbent):
            winners[event.idempotency_key] = event
    # Sorted by key so the same batch always produces the same MERGE source.
    return [winners[key] for key in sorted(winners)]


def _merge_rank(event: IngestionEvent) -> tuple[Any, str]:
    """Total order over events sharing an idempotency key: newest wins.

    ``event_id`` breaks a timestamp tie so the survivor is decided by the batch
    contents, never by the order Kafka happened to deliver the partition.
    """
    return (event.occurred_at, event.event_id)


def feast_online_request(asker_id: str) -> dict[str, Any]:
    """Build the Feast ``/get-online-features`` request for ``asker_activity_v1``."""
    return {
        # FEATURE_REFS is the registry contract; re-listing it here would let
        # the serving path and the feature view drift apart silently.
        "features": list(FEATURE_REFS),
        "entities": {FEAST_ENTITY_JOIN_KEY: [asker_id]},
        # Response keys stay short (``avg_rating``, not
        # ``asker_activity_v1__avg_rating``), which is what the parser expects.
        "full_feature_names": False,
    }


def readiness_status(probes: Iterable[dict[str, Any]]) -> str:
    """Return ``ready``, ``degraded`` or ``not_ready`` from probe severity."""
    degraded = False
    for probe in probes:
        if probe.get("ready"):
            continue
        if probe.get("mandatory"):
            # A mandatory failure dominates: the gateway must take this
            # instance out of rotation instead of serving partial answers.
            return "not_ready"
        degraded = True
    return "degraded" if degraded else "ready"
