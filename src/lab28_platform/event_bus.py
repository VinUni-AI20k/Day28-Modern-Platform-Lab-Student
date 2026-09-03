"""Kafka producers, consumers, dead-letter handling and replay.

Three rules this module exists to enforce:

1. **Trace context travels with the message.** W3C ``traceparent`` is written to
   the Kafka headers by the producer and re-established by the consumer, so the
   asynchronous hop does not break the end-to-end trace.
2. **Offsets are committed only after durable processing succeeds.** A crash
   between processing and commit replays the batch; idempotency downstream makes
   that safe. A crash after a failed batch never advances the offset.
3. **A poison message never blocks the partition.** After a bounded number of
   attempts the raw bytes go to the dead-letter topic with the failure category,
   and the offset advances. ``replay_dead_letters`` puts them back later.
"""

from __future__ import annotations

import base64
import json
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from confluent_kafka import Consumer, KafkaError, Message, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from opentelemetry.trace import SpanKind

from lab28_platform import integration_tasks
from lab28_platform.contracts import (
    TOPICS,
    DeadLetterEnvelope,
    ErrorCategory,
    IngestionEvent,
    ModelLifecycleEvent,
    ProcessedBatchEvent,
    TopicSpec,
)
from lab28_platform.settings import KafkaSettings
from lab28_platform.telemetry import (
    SPAN_KAFKA_CONSUME,
    SPAN_KAFKA_PRODUCE,
    context_from_kafka_headers,
    current_traceparent,
    inject_kafka_headers,
    span,
    traceparent_from_kafka_headers,
)

logger = logging.getLogger(__name__)

Publishable = IngestionEvent | ProcessedBatchEvent | ModelLifecycleEvent


class BrokerUnavailable(RuntimeError):
    """The broker could not be reached or refused the write."""


@dataclass(frozen=True)
class ConsumedMessage:
    """One decoded message plus the metadata the DLQ envelope needs."""

    event: IngestionEvent
    topic: str
    partition: int
    offset: int
    key: str | None
    traceparent: str | None
    headers: tuple[tuple[str, bytes | None], ...]


# --------------------------------------------------------------------------
# Topic administration
# --------------------------------------------------------------------------


def ensure_topics(
    settings: KafkaSettings, specs: Sequence[TopicSpec] = TOPICS, *, timeout: float = 20.0
) -> dict[str, str]:
    """Create the declared topics with their retention and cleanup policy.

    Declarative topic creation is what makes retention and partition count part
    of the reviewed configuration instead of an accident of first use.
    """
    admin = AdminClient({"bootstrap.servers": settings.bootstrap_servers})
    existing = set(admin.list_topics(timeout=timeout).topics)
    results: dict[str, str] = {}

    pending = [spec for spec in specs if spec.name not in existing]
    for spec in specs:
        if spec.name in existing:
            results[spec.name] = "exists"

    if not pending:
        return results

    futures = admin.create_topics(
        [
            NewTopic(
                spec.name,
                num_partitions=spec.partitions,
                replication_factor=spec.replication_factor,
                config=spec.config,
            )
            for spec in pending
        ]
    )
    for name, future in futures.items():
        try:
            future.result(timeout=timeout)
            results[name] = "created"
        except Exception as error:
            results[name] = f"failed: {error}"
    return results


def broker_metadata(settings: KafkaSettings, *, timeout: float = 3.0) -> dict[str, Any]:
    """Health probe used by /ready and the readiness report."""
    admin = AdminClient({"bootstrap.servers": settings.bootstrap_servers})
    metadata = admin.list_topics(timeout=timeout)
    return {
        "brokers": len(metadata.brokers),
        "topics": sorted(metadata.topics),
    }


# --------------------------------------------------------------------------
# Producer
# --------------------------------------------------------------------------


class EventPublisher:
    """Synchronous, at-least-once producer with trace headers.

    ``acks=all`` plus a flush-and-check makes a successful ``publish`` mean the
    broker durably accepted the record, which is what the API's 202 promises.
    """

    def __init__(self, settings: KafkaSettings) -> None:
        self._settings = settings
        self._producer = Producer(
            {
                "bootstrap.servers": settings.bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
                "retries": settings.max_delivery_attempts,
                "delivery.timeout.ms": int(settings.delivery_timeout_seconds * 1000),
                "linger.ms": 5,
            }
        )

    @property
    def healthy(self) -> bool:
        try:
            self._producer.list_topics(timeout=2)
        except Exception:
            return False
        return True

    def publish(self, topic: str, key: str, event: Publishable) -> None:
        """Publish one event, blocking until the broker acknowledges it."""
        errors: list[str] = []

        def on_delivery(error: KafkaError | None, _message: Message) -> None:
            if error is not None:
                errors.append(str(error))

        with span(
            SPAN_KAFKA_PRODUCE,
            kind=SpanKind.PRODUCER,
            attributes={
                "messaging.system": "kafka",
                "messaging.destination.name": topic,
                "messaging.kafka.message.key": key,
                "lab28.schema_version": event.schema_version,
            },
        ):
            headers = integration_tasks.event_headers(
                current_traceparent() or event.traceparent, key
            )
            headers.append(("schema_version", event.schema_version.encode("utf-8")))
            try:
                self._producer.produce(
                    topic,
                    key=key.encode("utf-8"),
                    value=event.model_dump_json().encode("utf-8"),
                    headers=headers,
                    on_delivery=on_delivery,
                )
                remaining = self._producer.flush(self._settings.delivery_timeout_seconds)
            except BufferError as error:
                raise BrokerUnavailable(f"producer queue is full: {error}") from error
            except Exception as error:
                raise BrokerUnavailable(f"produce failed: {error}") from error

        if remaining or errors:
            detail = errors[0] if errors else f"{remaining} message(s) undelivered"
            raise BrokerUnavailable(f"Kafka delivery failed: {detail}")

    def publish_dead_letter(self, envelope: DeadLetterEnvelope) -> None:
        errors: list[str] = []

        def on_delivery(error: KafkaError | None, _message: Message) -> None:
            if error is not None:
                errors.append(str(error))

        self._producer.produce(
            self._settings.topic_dlq,
            key=(envelope.original_key or "unknown").encode("utf-8"),
            value=envelope.model_dump_json().encode("utf-8"),
            headers=inject_kafka_headers(),
            on_delivery=on_delivery,
        )
        remaining = self._producer.flush(self._settings.delivery_timeout_seconds)
        if remaining or errors:
            raise BrokerUnavailable("dead-letter publish failed")

    def close(self) -> None:
        """Drain the producer on shutdown.

        ``publish`` already flushes per call, so this is normally a no-op — but
        it is what keeps that guarantee from depending on every future caller
        remembering to flush.
        """
        self._producer.flush(self._settings.delivery_timeout_seconds)


# --------------------------------------------------------------------------
# Consumer
# --------------------------------------------------------------------------


class BatchConsumer:
    """Manual-commit consumer that decodes and validates ``data.raw``.

    Auto-commit is disabled on purpose. ``commit()`` is called by the caller
    only after the Delta merge, the feature export and the vector upsert have
    all succeeded.
    """

    def __init__(self, settings: KafkaSettings, *, topic: str | None = None) -> None:
        self._settings = settings
        self._topic = topic or settings.topic_raw
        self._consumer = Consumer(
            {
                "bootstrap.servers": settings.bootstrap_servers,
                "group.id": settings.group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "session.timeout.ms": 45000,
                "max.poll.interval.ms": 300000,
            }
        )
        self._consumer.subscribe([self._topic])

    def poll_batch(
        self, max_messages: int, *, idle_polls: int = 10, poll_timeout: float = 1.0
    ) -> tuple[list[ConsumedMessage], list[DeadLetterEnvelope]]:
        """Poll up to ``max_messages``.

        Returns decoded messages and, separately, envelopes for messages whose
        payload could not be validated. Undecodable input is a permanent defect:
        retrying it would loop forever, so it goes straight to the dead-letter
        list and the offset is allowed to advance. Ten empty one-second polls
        leave room for a fresh consumer group to finish assignment before an
        Airflow run is treated as an empty batch.
        """
        decoded: list[ConsumedMessage] = []
        poison: list[DeadLetterEnvelope] = []
        idle = 0

        while len(decoded) + len(poison) < max_messages and idle < idle_polls:
            message = self._consumer.poll(poll_timeout)
            if message is None:
                idle += 1
                continue
            if message.error():
                error = message.error()
                if error.code() == KafkaError._PARTITION_EOF:
                    idle += 1
                    continue
                raise BrokerUnavailable(str(error))

            headers = tuple(message.headers() or ())
            traceparent = traceparent_from_kafka_headers(headers)
            raw = message.value() or b""
            try:
                event = IngestionEvent.model_validate_json(raw)
            except Exception as error:
                poison.append(
                    DeadLetterEnvelope(
                        original_topic=message.topic(),
                        original_partition=message.partition(),
                        original_offset=message.offset(),
                        original_key=_decode_key(message.key()),
                        error_category=ErrorCategory.VALIDATION,
                        error_detail=str(error)[:500],
                        attempts=1,
                        traceparent=traceparent,
                        raw_payload_b64=base64.b64encode(raw).decode("ascii"),
                    )
                )
                continue

            decoded.append(
                ConsumedMessage(
                    event=event,
                    topic=message.topic(),
                    partition=message.partition(),
                    offset=message.offset(),
                    key=_decode_key(message.key()),
                    traceparent=traceparent,
                    headers=headers,
                )
            )
        return decoded, poison

    def consume_span(self, message: ConsumedMessage):
        """Open a consumer span linked to the producer's trace context."""
        return span(
            SPAN_KAFKA_CONSUME,
            kind=SpanKind.CONSUMER,
            parent=context_from_kafka_headers(message.headers),
            attributes={
                "messaging.system": "kafka",
                "messaging.source.name": message.topic,
                "messaging.kafka.message.offset": message.offset,
                "lab28.idempotency_key": message.event.idempotency_key,
            },
        )

    def commit(self) -> None:
        self._consumer.commit(asynchronous=False)

    def close(self) -> None:
        self._consumer.close()


def _decode_key(key: bytes | None) -> str | None:
    return key.decode("utf-8", errors="replace") if key else None


# --------------------------------------------------------------------------
# Durable batch processing
# --------------------------------------------------------------------------


def process_batch_then_commit(
    consumer: BatchConsumer,
    publisher: EventPublisher,
    handler: Callable[[list[IngestionEvent]], int],
    *,
    max_messages: int = 200,
    max_attempts: int = 3,
) -> dict[str, int]:
    """Poll, process with bounded retry, dead-letter the rest, then commit.

    The commit happens once, at the end, and only if no exception escaped. That
    is the invariant the "no data loss" journey asserts: kill the process at any
    point before the commit and the same events are redelivered.
    """
    messages, poison = consumer.poll_batch(max_messages)
    for envelope in poison:
        publisher.publish_dead_letter(envelope)

    processed = 0
    dead_lettered = len(poison)

    if messages:
        # One consumer span per message, opened from the *producer's* headers.
        # This is the hop where trace context is normally lost — nothing carries
        # it across a broker unless someone writes it and reads it back — so the
        # span is emitted here rather than left to whichever caller remembers.
        events = []
        for message in messages:
            with consumer.consume_span(message) as active:
                active.set_attribute("lab28.event.kind", message.event.kind)
                events.append(message.event)

        last_error: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            try:
                processed = handler(events)
                last_error = None
                break
            except Exception as error:
                last_error = error
                logger.warning("batch attempt %s/%s failed: %s", attempt, max_attempts, error)

        if last_error is not None:
            category = _classify(last_error)
            if category is ErrorCategory.DEPENDENCY_UNAVAILABLE:
                # A dependency outage is transient. Do not dead-letter good
                # events and do not commit: let the next run redeliver them.
                raise last_error
            for message in messages:
                publisher.publish_dead_letter(
                    DeadLetterEnvelope(
                        original_topic=message.topic,
                        original_partition=message.partition,
                        original_offset=message.offset,
                        original_key=message.key,
                        error_category=category,
                        error_detail=str(last_error)[:500],
                        attempts=max_attempts,
                        traceparent=message.traceparent,
                        raw_payload_b64=base64.b64encode(
                            message.event.model_dump_json().encode("utf-8")
                        ).decode("ascii"),
                    )
                )
                dead_lettered += 1

    if messages or poison:
        consumer.commit()

    return {
        "polled": len(messages) + len(poison),
        "processed": processed,
        "dead_lettered": dead_lettered,
    }


def _classify(error: Exception) -> ErrorCategory:
    if isinstance(error, BrokerUnavailable):
        return ErrorCategory.DEPENDENCY_UNAVAILABLE
    text = str(error).lower()
    if "timeout" in text or "timed out" in text:
        return ErrorCategory.DEPENDENCY_TIMEOUT
    if "connection" in text or "refused" in text or "unavailable" in text:
        return ErrorCategory.DEPENDENCY_UNAVAILABLE
    if "schema" in text or "validation" in text:
        return ErrorCategory.VALIDATION
    return ErrorCategory.INTERNAL


# --------------------------------------------------------------------------
# Replay
# --------------------------------------------------------------------------


def replay_dead_letters(
    settings: KafkaSettings, *, limit: int = 100, group_suffix: str = "replay"
) -> dict[str, int]:
    """Re-publish dead-lettered payloads to the original topic.

    Replay is an operator action, not an automatic one: the defect that caused
    the dead-letter must be fixed first, otherwise the message loops back.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": f"{settings.group_id}-{group_suffix}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([settings.topic_dlq])
    publisher = EventPublisher(settings)

    replayed = 0
    skipped = 0
    idle = 0
    try:
        while replayed + skipped < limit and idle < 3:
            message = consumer.poll(1.0)
            if message is None:
                idle += 1
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    idle += 1
                    continue
                raise BrokerUnavailable(str(message.error()))
            try:
                envelope = DeadLetterEnvelope.model_validate_json(message.value() or b"")
                payload = base64.b64decode(envelope.raw_payload_b64)
                event = IngestionEvent.model_validate_json(payload)
            except Exception:
                skipped += 1
                continue
            publisher.publish(envelope.original_topic, event.idempotency_key, event)
            replayed += 1
        consumer.commit(asynchronous=False)
    finally:
        consumer.close()

    return {"replayed": replayed, "skipped": skipped}


def dead_letter_count(settings: KafkaSettings, *, timeout: float = 5.0) -> int:
    """Count messages currently parked on the dead-letter topic."""
    consumer = Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": f"{settings.group_id}-dlq-count",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    try:
        metadata = consumer.list_topics(settings.topic_dlq, timeout=timeout)
        topic_metadata = metadata.topics.get(settings.topic_dlq)
        if topic_metadata is None or topic_metadata.error is not None:
            return 0
        from confluent_kafka import TopicPartition

        total = 0
        for partition_id in topic_metadata.partitions:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(settings.topic_dlq, partition_id), timeout=timeout
            )
            total += max(0, high - low)
        return total
    finally:
        consumer.close()


def decode_dead_letters(settings: KafkaSettings, *, limit: int = 20) -> list[dict[str, Any]]:
    """Read dead letters for the runbook and the failure-injection evidence."""
    consumer = Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": f"{settings.group_id}-dlq-read",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([settings.topic_dlq])
    found: list[dict[str, Any]] = []
    idle = 0
    try:
        while len(found) < limit and idle < 3:
            message = consumer.poll(1.0)
            if message is None:
                idle += 1
                continue
            if message.error():
                idle += 1
                continue
            try:
                found.append(json.loads((message.value() or b"{}").decode("utf-8")))
            except json.JSONDecodeError:
                continue
    finally:
        consumer.close()
    return found
