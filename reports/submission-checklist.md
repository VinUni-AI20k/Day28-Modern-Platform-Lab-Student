# Submission checklist

This report indexes the artefacts required by `SUBMISSION.md`. Values below
come from the final live verification run; the source JSON files remain the
authoritative records.

## Required artefacts

| Requirement | Submission artefact | Status |
|---|---|---|
| Integration report | `evidence/integration-report.json` | Present; 6/6 directly probed points pass, score 100. |
| Ten integration points | `evidence/ip01-*.json` through `evidence/ip10-*.json` | Present; IP09 has separate Prometheus and Grafana files. |
| Architecture and ownership | `docs/images/lab28-architecture-overview.svg`, `contracts/integration-matrix.yaml` | Present. |
| Happy path trace | `evidence/ip10-trace.json` | Present; trace `28fb258ebd7f47e6abbd15040541da4d`, 26 spans, no required span missing. |
| Pipeline run | `evidence/ip02-airflow-run.json` | Present; run `it-5d0ef22d`, all four tasks succeeded. |
| Delta and release provenance | `evidence/ip03-delta-history.json`, `evidence/ip06-mlflow-release.json` | Present; feedback Delta version 61 and MLflow champion version 1. |
| Failure/recovery and no-loss proof | `integration-tests/test_j4_degraded_recovery.py`, `evidence/ip02-airflow-run.json`, `evidence/ip03-delta-history.json` | Verified; J4 passed 13/13. |
| Load profile | this report and `load-tests/run_profile.py` | Verified for `/ready`: 200 requests, 8 workers, all 200; P50 882.99 ms, P95 993.07 ms, P99 1451.23 ms. |
| Kubernetes/GitOps | `deploy/kubernetes/`, `gitops/application.yaml`, `runbooks/gitops-rollback.md` | Static manifest validation passed; live Argo drift is not claimed. |
| Trade-offs and contributions | `ANSWERS.md` | Present. |

## Validation record

- GPU integration validation: `71 passed, 1 deselected` (`langsmith` was
  deselected because no credential was provided).
- J4 failure/recovery validation: `13 passed`.
- Submission validation: Ruff passed; matrix verification passed 245 checks;
  portability and Kubernetes/GitOps manifest contracts passed; unit suite passed
  83 tests; non-GPU fast integration suite passed 56 tests (16 GPU/LangSmith
  tests deselected by environment).
- vLLM identity: vLLM 0.28.0, Qwen/Qwen3-1.7B, 111 native `vllm:` metrics;
  see `evidence/ip07-vllm-identity.json`.
- Trace coverage: 11 required spans and 4 services; see
  `evidence/ip10-trace.json`.

## Honest limitations

The local `lab28 integration` command marks IP02, IP08, IP09 and IP10 as
`unverified` because a serving process cannot inspect those external systems.
Their corresponding evidence files above provide the required independent
proof. LangSmith and live Argo CD drift/self-heal remain unverified without
their external credential/cluster.
