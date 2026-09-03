# Lab 28 answers

## Implementation decisions and trade-offs

1. `event_headers` always sends the idempotency key as bytes and only sends
   `traceparent` when one exists. This preserves Kafka/W3C context without inventing
   an empty trace value.
2. `dedupe_latest` keeps the greatest `(occurred_at, event_id)` for each
   `idempotency_key`, then sorts by key. The event ID makes ties deterministic and
   the final ordering makes replay results reproducible.
3. `feast_online_request` imports the canonical `FEATURE_REFS` instead of copying
   feature names. This avoids contract drift, at the cost of coupling the request
   adapter to the shared contract module.
4. `readiness_status` fails closed for a failed mandatory dependency, degrades for
   an optional dependency, and reports ready otherwise. This keeps retrieval
   failures out of rotation while allowing an explicitly degraded response when
   only optional enrichment/inference is unavailable.
5. Kafka delivery is at-least-once. Correctness therefore comes from retaining all
   deliveries in Kafka and making the Delta/Qdrant sinks idempotent, rather than
   pretending the broker provides exactly-once processing across every system.

## Verified results

- Unit/configuration gate: 87 tests passed; Ruff, integration-matrix,
  portability and Kubernetes manifest validation passed.
- J1: 12 local non-GPU checks passed (gateway → Kafka → Airflow → Delta →
  Feast/Qdrant, MLflow, Prometheus and Jaeger).
- J2: 9 checks passed; replay retained broker deliveries but produced one Delta
  row and one Qdrant point per idempotency key.
- J3: 6 non-GPU checks passed; MLflow champion promotion and rollback resolved.
- J4: 9 non-GPU checks passed; degraded/fail-closed behavior, DLQ, replay and
  no-data-loss recovery resolved.
- J5: 9 local checks passed; request/latency/readiness metrics and synchronous/
  asynchronous trace continuity resolved.
- Gateway rate limit: 5 checks passed. Prometheus/Grafana: 5 non-GPU checks passed.
- Direct API load profile, 200 requests/8 workers: 200 HTTP 200; P50 573.11 ms,
  P95 775.97 ms and P99 939.46 ms.

Exact live IDs and payloads are in `evidence/`.

## Production gaps

- IP07 is **UNVERIFIED**: this laptop has no real GPU-backed vLLM endpoint. The
  evidence records the failed identity probe; no compatible mock was used.
- The LangSmith export leg of IP10 is **UNVERIFIED** because no
  `LANGSMITH_API_KEY` was supplied. Local OTLP/Jaeger continuity is verified.
- Kubernetes and Argo CD manifests pass static validation, but live drift/self-heal
  is **UNVERIFIED** because no Kubernetes/Argo CD context is available locally.
- Compose uses single-host local state and development credentials. Production
  needs managed secrets, TLS/mTLS, durable HA stores, backups, retention policies,
  image signing/scanning and tested restore procedures.
- `/ready` performs dependency probes and becomes relatively expensive under
  concurrency. A production deployment should cache probe results briefly, bound
  per-dependency timeouts and keep liveness independent from readiness.
- Envoy's local burst limit intentionally rejects a large unpaced profile. Tune it
  from measured service capacity and use a distributed limiter for multiple gateway
  replicas.

## Individual contribution

Completed the four student-owned integration functions in
`src/lab28_platform/integration_tasks.py`; ran the Python/configuration gates;
brought up and diagnosed the full Docker stack; repaired the generated FastEmbed
cache interoperability issue; executed J1–J5, recovery, observability and load
checks; and assembled the evidence and demo documentation. Scaffolded platform
components outside that file remain the supplied lab implementation.
