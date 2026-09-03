# Day 28 reflection

## Trade-offs

- The platform keeps ingestion, the lakehouse, online features, vector search,
  model registry, and serving as separate components.  This adds operational
  overhead, but it makes replay-safe writes, model rollback, and component
  readiness independently observable.
- On this machine the core/full Docker profile is used without a GPU-backed
  vLLM endpoint.  IP07 is therefore reported as **UNVERIFIED**, not simulated.
  The rest of the platform remains usable in `degraded` mode and the evidence
  records the actual `ConnectError`.
- The load probe is intentionally small and uses only the standard library.
  It is reproducible, but it does not replace a sustained capacity test with
  resource telemetry and a production traffic mix.

## Production gaps and next improvements

- Configure a real GPU-backed vLLM endpoint and verify `/version`, `/v1/models`,
  and `vllm:` metrics before claiming IP07 complete.
- Run Argo CD against an actual Kubernetes cluster to capture drift detection,
  self-healing, and desired-revision rollback.  This workspace has no Kubernetes
  current context, so a live GitOps reconciliation has not been claimed.
- Add a rate-limit-aware load profile, dashboard correlation, and a capacity
  target.  The 200-request/8-worker probe showed rejected/failed probe requests
  at the gateway; this should be investigated before production rollout.
- Store the evidence bundle in the approved submission location, not Git.  The
  local `evidence/` directory is intentionally ignored.

## Individual contribution

This was completed as an individual lab.  The work covered the four starter
functions (Kafka headers/trace propagation, Delta deduplication, Feast request
construction, and readiness classification), Docker full-stack operation,
integration-test execution, evidence collection, load profiling, incident
recovery, and MLflow promotion/rollback verification.
