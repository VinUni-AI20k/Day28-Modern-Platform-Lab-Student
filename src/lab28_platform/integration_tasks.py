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
    headers: list[tuple[str, bytes]] = [
        ("idempotency-key", idempotency_key.encode("utf-8")),
    ]
    # Falsy rather than ``is None``: the producer passes
    # ``current_traceparent() or event.traceparent``, so an inactive trace
    # arrives as "". An empty traceparent breaks W3C parsing downstream.
    if traceparent:
        headers.append(("traceparent", traceparent.encode("utf-8")))
    return headers


def dedupe_latest(events: Iterable[IngestionEvent]) -> list[IngestionEvent]:
    """Return one newest event per idempotency key, in deterministic key order.

    Compare ``(occurred_at, event_id)`` so ties do not depend on Kafka delivery
    order.  The Spark Delta MERGE calls this through ``delta_store``.
    """
    newest: dict[str, IngestionEvent] = {}
    # One pass: callers hand over a generator, so ``events`` is single-use.
    for event in events:
        current = newest.get(event.idempotency_key)
        if current is None or (event.occurred_at, event.event_id) > (
            current.occurred_at,
            current.event_id,
        ):
            newest[event.idempotency_key] = event
    # Insertion order is Kafka's order; sort so a replay yields the same batch.
    return [newest[key] for key in sorted(newest)]


def feast_online_request(asker_id: str) -> dict[str, Any]:
    """Build the Feast ``/get-online-features`` request for ``asker_activity_v1``."""
    return {
        "entities": {"asker_id": [asker_id]},
        # Single source of truth: contracts.FEATURE_REFS, never a second copy.
        "features": list(FEATURE_REFS),
        # Short names keep the response parseable by ``_to_lookup``.
        "full_feature_names": False,
    }


def readiness_status(probes: Iterable[dict[str, Any]]) -> str:
    """Return ``ready``, ``degraded`` or ``not_ready`` from probe severity."""
    degraded = False
    # One pass: ``serving_readiness`` passes a generator expression.
    for probe in probes:
        if not probe["ready"]:
            if probe["mandatory"]:
                return "not_ready"
            degraded = True
    return "degraded" if degraded else "ready"
