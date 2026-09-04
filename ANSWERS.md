# Submission answers

## Trade-offs

- The lab keeps Kafka, Delta, Feast, Qdrant and MLflow locally reproducible in
  Docker. Real inference is a GPU-gated boundary: the verification run used
  vLLM 0.28 with `Qwen/Qwen3-1.7B`, then removed the temporary GPU endpoint.
- Readiness is intentionally stricter than liveness. A missing vector store
  makes `/ready` fail closed, while a missing feature store produces a
  degraded-but-served answer with the reason recorded in the response.
- Delta `MERGE` and deterministic Qdrant point IDs make replay idempotent. The
  trade-off is higher pipeline latency in return for recoverable writes and
  auditable versions.

## Production gaps

- The local load result is a readiness benchmark, not a capacity claim for
  grounded inference. A production test must separately load `/api/v1/ask`
  with a representative corpus, measured GPU queue depth and token throughput.
- LangSmith export was not verified because no `LANGSMITH_API_KEY` was supplied.
  Local Jaeger evidence is included instead; no synthetic LangSmith evidence
  is claimed.
- Kubernetes manifests and the GitOps rollback runbook are validated locally.
  Live Argo CD drift/self-heal requires an approved cluster and is therefore
  not claimed as executed in this submission.
- The temporary GPU host was removed after evidence collection. A production
  deployment should use a managed GPU pool, persistent model cache, access
  control and cost limits.

## Contributions

| Member | Contribution |
|---|---|
| Dien Manh Hung | Implemented and verified the integration boundaries, recovery tests, real-vLLM evidence, observability evidence, manifests and submission pack. |

