"""Four student-owned boundaries used by the live platform.

Run ``uv run pytest starter-tests -q`` while completing these functions.  Do
not change their signatures: Kafka, Delta, Feast and ``/ready`` call them.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from lab28_platform.contracts import FEATURE_REFS, IngestionEvent


def event_headers(
    traceparent: str | None, idempotency_key: str
) -> list[tuple[str, bytes]]:
    """Return byte-valued Kafka headers for trace and replay correlation.

    ``idempotency-key`` is always required.  Omit ``traceparent`` when no trace
    is active rather than sending an empty, invalid W3C header.
    """
    headers = [("idempotency-key", idempotency_key.encode("utf-8"))]
    if traceparent is not None:
        headers.insert(0, ("traceparent", traceparent.encode("utf-8")))
    return headers


def dedupe_latest(events: Iterable[IngestionEvent]) -> list[IngestionEvent]:
    """Return one newest event per idempotency key, in deterministic key order.

    Compare ``(occurred_at, event_id)`` so ties do not depend on Kafka delivery
    order.  The Spark Delta MERGE calls this through ``delta_store``.
    """
    latest_by_key: dict[str, IngestionEvent] = {}
    for event in events:
        current = latest_by_key.get(event.idempotency_key)
        if current is None or (event.occurred_at, event.event_id) > (
            current.occurred_at,
            current.event_id,
        ):
            latest_by_key[event.idempotency_key] = event
    return [latest_by_key[key] for key in sorted(latest_by_key)]


def feast_online_request(asker_id: str) -> dict[str, Any]:
    """Build the Feast ``/get-online-features`` request for ``asker_activity_v1``."""
    return {
        "entities": {"asker_id": [asker_id]},
        "features": list(FEATURE_REFS),
        "full_feature_names": False,
    }


def readiness_status(probes: Iterable[dict[str, Any]]) -> str:
    """Return ``ready``, ``degraded`` or ``not_ready`` from probe severity."""
    has_optional_failure = False
    for probe in probes:
        if not probe["ready"]:
            if probe["mandatory"]:
                return "not_ready"
            has_optional_failure = True
    return "degraded" if has_optional_failure else "ready"
