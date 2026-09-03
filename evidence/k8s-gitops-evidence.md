# Kubernetes and GitOps evidence

- `uv run python scripts/validate_manifests.py` returned:
  `Kubernetes and GitOps manifest contracts passed`.
- Required resources are present: Deployment, Service, ServiceAccount, ConfigMap,
  HPA, PDB, NetworkPolicy, Gateway and HTTPRoute.
- The API image is pinned to `ghcr.io/vinuni-ai20k/day28-platform-api:3.0.0` and
  includes startup, readiness and liveness probes, resource bounds, non-root,
  read-only filesystem, dropped capabilities and no privilege escalation.
- Argo CD pins `targetRevision: refs/tags/v3.0.0`, enables automated pruning and
  self-healing, and retains five revisions. A desired-state rollback is performed
  by reverting the pinned tag/image in Git; Argo CD then reconciles that revision.
- Live drift injection/self-heal is **UNVERIFIED** on this machine: neither a
  Kubernetes context nor the `kubectl`/`argocd` clients were available. No live
  cluster result is claimed.
