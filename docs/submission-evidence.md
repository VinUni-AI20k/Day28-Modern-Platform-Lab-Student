# Submission evidence record

This record indexes evidence generated from the local full Docker stack on
2026-09-03.  Runtime evidence remains in the ignored `evidence/` directory and
must be attached to the private submission separately; it must not be committed.

## Validation results

| Check | Result |
|---|---|
| `uv run ruff check .` | passed |
| `uv run python scripts/verify_matrix.py` | 245 passed |
| `uv run python scripts/check_portability.py` | passed |
| `uv run python scripts/validate_manifests.py` | passed |
| `uv run pytest tests -q --basetemp .lab28/pytest-submission` | 83 passed |
| J1 golden path | 12 passed, 3 GPU-gated skips |
| J2 idempotent replay | 9 passed |
| J3 promotion/rollback | 6 passed, 3 GPU-gated skips |
| J4 degraded recovery | 9 passed, 4 GPU-gated skips |
| J5 trace/metrics continuity | 9 passed, 1 GPU-gated skip |

## Happy path

- Airflow run: `it-13a97024`, state `success` (`evidence/ip02-airflow-run.json`).
- Trace ID: `76ea58936cc3459c83566a8edd0592c9` in that DAG run's W3C
  `traceparent`.
- Delta: `evidence/ip03-delta-history.json` records a monotonic Delta MERGE
  history.  The latest recorded feedback version is 11 in that snapshot.
- MLflow: `evidence/ip06-mlflow-release.json` records champion release version
  5, run ID `44c56c3d6e824a12881ebf61fef26efb`, and Delta provenance version 14.

The versions are collected at different points in the live test run, so they
are provenance records, not a claim that all values are one atomic timestamp.

## Incident and recovery

J4 injected two controlled dependency failures and a malformed Kafka record.

1. Stopping Feast did not change the API to `not_ready`; restoring the
   container restored its probe.
2. Stopping Qdrant made `/ready` fail closed as `not_ready` with HTTP 503;
   after restoration the API returned to its baseline readiness verdict.
3. A malformed record was parked in the dead-letter topic while a good record
   in the same batch reached Delta.  A valid parked event was replayed; Delta
   contained exactly one row for its idempotency key.  J4 finished with 9
   passing tests.  GPU-only request-serving assertions were skipped honestly
   because no verifiable vLLM endpoint is configured.

This is the no-data-loss proof: the bad record is retained in DLQ, the good
record is persisted, and replay does not duplicate the valid row.

## Release rollback and GitOps

J3 registered a new MLflow release, moved the `champion` alias, then restored
the previous alias.  The test passed 6 non-GPU assertions; the generated MLflow
evidence includes version, run ID, model/data provenance, and `promoted_from`.

Kubernetes/GitOps manifests pass static contract validation.  A live
drift/self-heal demonstration is **UNVERIFIED** because `kubectl` has no
current Kubernetes context in this environment.  No drift or Argo CD result is
fabricated.  The intended procedure is documented in
`runbooks/gitops-rollback.md`.

## Load profile and bottleneck observation

| Probe | HTTP status counts | P50 | P95 | P99 |
|---|---|---:|---:|---:|
| 50 requests, 1 worker | 200: 50 | 300.68 ms | 378.79 ms | 385.63 ms |
| 200 requests, 8 workers | 200: 67; probe failures: 133 | 7.40 ms | 840.42 ms | 1260.93 ms |

The concurrent probe exposes gateway admission/rate-limit behavior as the
first bottleneck: a substantial portion did not complete as HTTP 200 and tail
latency rose sharply.  Before production use, inspect Envoy rate-limit metrics,
define an acceptable rejection policy, and repeat with sustained workload plus
CPU/memory telemetry.

## Evidence index

The evidence bundle contains `integration-report.json` and the required
IP01--IP10 filenames: `ip01-kafka-consume.json`, `ip02-airflow-run.json`,
`ip03-delta-history.json`, `ip04-feast-online.json`,
`ip05-qdrant-search.json`, `ip06-mlflow-release.json`,
`ip07-vllm-identity.json`, `ip08-gateway.json`,
`ip09-prometheus-targets.json`, and `ip10-trace.json`.

IP07 remains `UNVERIFIED`: its file records `unreachable: ConnectError` rather
than pretending to be a vLLM server.  The local trace evidence covers gateway,
API, Kafka, Airflow, and Delta; spans needing a real serving request remain
unverified with the absent vLLM endpoint.
