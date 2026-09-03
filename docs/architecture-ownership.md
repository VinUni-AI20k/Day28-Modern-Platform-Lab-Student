# Architecture and ownership

```mermaid
flowchart LR
    U[Client] -->|IP08 · team-platform| G[Envoy gateway]
    G -->|HTTP + request/trace ID| A[FastAPI]
    A -->|IP01 · team-ingestion| K[Kafka data.raw]
    K -->|IP02 · team-ingestion| F[Airflow]
    F -->|IP03 · team-data| D[Delta Lake]
    D -->|IP04 · team-data| FE[Feast]
    D -->|IP05 · team-serving| Q[Qdrant]
    D -->|IP06 · team-data| M[MLflow Registry]
    A --> FE
    A --> Q
    A --> M
    A -->|IP07 · team-serving| V[vLLM]
    O[OpenTelemetry + Jaeger] -. IP10 · team-platform .- G
    O -. traces .- A
    O -. traces .- F
    P[Prometheus + Grafana] -. IP09 · team-platform .- G
    P -. metrics .- A
    P -. metrics .- K
```

| Owner | Integration points | Responsibility |
|---|---|---|
| `team-ingestion` | IP01–IP02 | HTTP events, Kafka contracts, Airflow, retry and DLQ/replay |
| `team-data` | IP03–IP04, IP06 | Delta MERGE/time travel, Feast materialization, MLflow release/rollback |
| `team-serving` | IP05, IP07 | Hybrid retrieval, deterministic vector IDs, grounded inference and fallback |
| `team-platform` | IP08–IP10 | Envoy policy, readiness, metrics/dashboards, OTLP traces and GitOps |
| `team-presenter` | all | Evidence index, incident narrative, demo order and Q&A |

This submission was completed individually. The role split above is retained so an
incident can still be routed to the correct system boundary during the demo.
