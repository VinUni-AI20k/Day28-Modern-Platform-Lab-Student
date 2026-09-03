"""Readiness probes for the serving path and the ten integration points.

Two audiences, two shapes, one set of probes.

``serving_readiness`` answers the ``/ready`` endpoint. It reports only the
dependencies the serving path actually needs, because that verdict removes a pod
from the gateway's rotation — including a component the request path can survive
without would take healthy pods out of service for no reason. So Feast is
reported but not mandatory: a cold feature store degrades an answer, it does not
invalidate one.

``integration_report`` answers ``lab28 readiness``. It reads the integration
matrix and attributes each probe to the slide's integration points, which is the
artefact a team shows at the Milestone 3 demo.

Every heavy client is imported inside the probe that uses it. ``qdrant-client``,
``mlflow`` and ``deltalake`` are optional extras, and ``lab28 preflight`` has to
run on a base install — before the student has installed anything else — to tell
them which execution profile they can use.
"""

from __future__ import annotations

import os
import platform
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from lab28_platform import integration_tasks, metrics
from lab28_platform.contracts import (
    SERVICE_OWNERS,
    ReadinessComponent,
    ReadinessResponse,
)
from lab28_platform.settings import Settings
from lab28_platform.telemetry import current_trace_id

#: Where the machine-readable integration matrix lives, relative to the repo root.
MATRIX_PATH = Path("contracts/integration-matrix.yaml")


@dataclass(frozen=True)
class Probe:
    """One dependency probe.

    ``mandatory`` is the difference between "this pod cannot serve" and "this
    pod will serve a degraded answer". Only mandatory failures make ``/ready``
    return 503.
    """

    name: str
    ready: bool
    detail: str
    mandatory: bool = True

    def to_component(self) -> ReadinessComponent:
        return ReadinessComponent(
            name=self.name,
            ready=self.ready,
            detail=self.detail,
            owner=SERVICE_OWNERS.get(self.name, "team-platform"),
        )


def _unavailable(name: str, error: Exception, *, mandatory: bool = True) -> Probe:
    """Render any probe failure as a not-ready component instead of raising.

    A readiness endpoint that raises tells the operator nothing. A readiness
    endpoint that says which dependency failed and how tells them where to look.
    """
    if isinstance(error, ImportError):
        detail = f"optional dependency missing: {error}"
    else:
        detail = f"{type(error).__name__}: {error}"
    return Probe(name=name, ready=False, detail=detail, mandatory=mandatory)


# -- individual probes -----------------------------------------------------


def probe_kafka(settings: Settings) -> Probe:
    """IP01 — the broker answers and every declared topic exists."""
    from lab28_platform.contracts import TOPICS
    from lab28_platform.event_bus import broker_metadata

    try:
        metadata = broker_metadata(settings.kafka, timeout=6.0)
    except Exception as error:
        return _unavailable("kafka", error)

    declared = {topic.name for topic in TOPICS}
    missing = sorted(declared - set(metadata["topics"]))
    if missing:
        return Probe("kafka", False, f"missing topics: {', '.join(missing)}")
    return Probe(
        "kafka",
        True,
        f"{metadata['brokers']} broker(s); {len(declared)} declared topics present",
    )


def probe_delta(settings: Settings) -> Probe:
    """IP03 — both tables have a readable transaction log."""
    try:
        from lab28_platform import delta_store

        report = delta_store.health(
            {"feedback": settings.feedback_table, "documents": settings.document_table}
        )
    except Exception as error:
        return _unavailable("spark-delta", error)

    if not report["reachable"]:
        broken = [
            f"{name}: {info.get('error', 'unreadable')}"
            for name, info in report["tables"].items()
            if "error" in info
        ]
        return Probe("spark-delta", False, "; ".join(broken))
    versions = ", ".join(
        f"{name} v{info['version']} ({info['rows']} rows)"
        for name, info in sorted(report["tables"].items())
    )
    return Probe("spark-delta", True, versions)


def probe_feast(settings: Settings, client: Any | None = None) -> Probe:
    """IP04 — degradable on purpose: cold features must not fail a pod."""
    try:
        from lab28_platform.feature_store import FeatureClient

        probe_client = client or FeatureClient(settings.feast)
        report = probe_client.health()
        if client is None:
            probe_client.close()
    except Exception as error:
        return _unavailable("feast", error, mandatory=False)

    return Probe("feast", bool(report["healthy"]), str(report["detail"]), mandatory=False)


def probe_qdrant(settings: Settings, client: Any | None = None) -> Probe:
    """IP05 — the collection exists and holds points."""
    try:
        from lab28_platform.vector_store import VectorStore

        store = client or VectorStore(settings.qdrant)
        report = store.health()
        if client is None:
            store.close()
    except Exception as error:
        return _unavailable("qdrant", error)

    ready = bool(report["reachable"] and report["collection_exists"] and report["points"])
    return Probe("qdrant", ready, f"{report['points']} points; {report['detail']}")


def probe_mlflow(settings: Settings, client: Any | None = None) -> Probe:
    """IP06 — the registry answers and a champion release resolves."""
    try:
        from lab28_platform.model_registry import ReleaseRegistry

        registry = client or ReleaseRegistry(settings.mlflow)
        report = registry.health()
    except Exception as error:
        return _unavailable("mlflow", error)

    return Probe("mlflow", bool(report["has_champion"]), str(report["detail"]))


def probe_vllm(settings: Settings) -> Probe:
    """IP07 — a real vLLM build, not merely an OpenAI-compatible endpoint."""
    try:
        from lab28_platform.llm_client import probe_identity

        identity = probe_identity(settings.vllm)
    except Exception as error:
        return _unavailable("vllm", error, mandatory=settings.vllm.require_real)

    if settings.vllm.require_real and not identity.is_real_vllm:
        return Probe(
            "vllm",
            False,
            f"not a verifiable vLLM server: {identity.detail}",
            mandatory=True,
        )
    return Probe(
        "vllm",
        identity.reachable,
        identity.detail,
        mandatory=settings.vllm.require_real,
    )


# -- serving readiness (/ready) --------------------------------------------


def serving_probes(
    settings: Settings,
    *,
    features: Any | None = None,
    vectors: Any | None = None,
    registry: Any | None = None,
) -> list[Probe]:
    """Probe every dependency of the request path.

    The API passes the clients it already holds so a readiness check reuses live
    connections instead of building and discarding a new client per probe.
    """
    return [
        probe_kafka(settings),
        probe_mlflow(settings, registry),
        probe_qdrant(settings, vectors),
        probe_vllm(settings),
        probe_feast(settings, features),
    ]


def serving_readiness(
    settings: Settings,
    *,
    features: Any | None = None,
    vectors: Any | None = None,
    registry: Any | None = None,
) -> ReadinessResponse:
    """Build the ``/ready`` body and publish per-component gauges."""
    probes = serving_probes(
        settings, features=features, vectors=vectors, registry=registry
    )
    components = [probe.to_component() for probe in probes]
    metrics.set_component_ready(
        (component.name, component.owner, component.ready) for component in components
    )

    status = integration_tasks.readiness_status(
        {"ready": probe.ready, "mandatory": probe.mandatory} for probe in probes
    )

    metrics.READINESS_SCORE.labels(pillar="serving", profile="request-path").set(
        sum(probe.ready for probe in probes) / len(probes)
    )
    return ReadinessResponse(
        status=status,  # type: ignore[arg-type]
        components=components,
        trace_id=current_trace_id(),
    )


# -- integration-point report (lab28 readiness) ----------------------------


def load_matrix(path: Path | None = None) -> dict[str, Any]:
    """Read the integration matrix that defines what the demo must prove."""
    source = path or MATRIX_PATH
    if not source.is_file():
        raise FileNotFoundError(f"integration matrix not found at {source}")
    return yaml.safe_load(source.read_text(encoding="utf-8"))


def integration_report(settings: Settings, *, matrix_path: Path | None = None) -> dict[str, Any]:
    """Attribute live probe results to the slide's ten integration points.

    Points the platform cannot prove from inside this process — the gateway,
    Prometheus and the trace backend — are reported ``unverified`` rather than
    passed. Claiming a green check for something never probed is the one failure
    mode a readiness report must not have.
    """
    matrix = load_matrix(matrix_path)
    probes = {probe.name: probe for probe in serving_probes(settings)}
    probes["spark-delta"] = probe_delta(settings)

    #: Which probe proves which integration point.
    by_point = {
        "IP01": "kafka",
        "IP03": "spark-delta",
        "IP04": "feast",
        "IP05": "qdrant",
        "IP06": "mlflow",
        "IP07": "vllm",
    }

    points: list[dict[str, Any]] = []
    for entry in matrix["points"]:
        point_id = entry["id"]
        component = by_point.get(point_id)
        if component is None:
            points.append(
                {
                    "id": point_id,
                    "name": entry["slide_name"],
                    "owner": entry["owner"],
                    "status": "unverified",
                    "detail": (
                        "not probed from the serving process; prove with "
                        f"{entry['demo_evidence'].split(' — ')[0]}"
                    ),
                    "readiness_check": entry["readiness_check"],
                }
            )
            continue
        probe = probes[component]
        points.append(
            {
                "id": point_id,
                "name": entry["slide_name"],
                "owner": entry["owner"],
                "status": "ready" if probe.ready else "not_ready",
                "detail": probe.detail,
                "readiness_check": entry["readiness_check"],
            }
        )

    verified = [point for point in points if point["status"] != "unverified"]
    passing = [point for point in verified if point["status"] == "ready"]
    return {
        "ready": len(passing) == len(verified) and bool(verified),
        "verified_points": len(verified),
        "passing_points": len(passing),
        "unverified_points": len(points) - len(verified),
        "score": round(100 * len(passing) / len(verified)) if verified else 0,
        "pillars": _pillar_scores(verified),
        "points": points,
    }


def _pillar_scores(verified: list[dict[str, Any]]) -> dict[str, float]:
    """Group verified points by the pillar prefix of their readiness check.

    The matrix names every check ``<pillar>.<check>`` — ``reliability``,
    ``security``, ``observability``, ``operations``. Scoring per pillar is what
    turns "7 of 10 green" into "observability is the gap", which is the sentence
    a team can act on.
    """
    buckets: dict[str, list[bool]] = {}
    for point in verified:
        pillar = str(point["readiness_check"]).split(".", 1)[0]
        buckets.setdefault(pillar, []).append(point["status"] == "ready")

    scores: dict[str, float] = {}
    for pillar, results in sorted(buckets.items()):
        score = sum(results) / len(results)
        metrics.READINESS_SCORE.labels(pillar=pillar, profile="integration").set(score)
        scores[pillar] = round(score, 3)
    return scores


# -- environment preflight -------------------------------------------------


def run_preflight() -> dict[str, object]:
    """Pick an execution profile before anything is installed.

    Runs on a base install with no extras, because its whole job is to tell a
    student whether this machine can run the stack locally at all.
    """
    free_gib = shutil.disk_usage(Path.cwd()).free / 1024**3
    cpu_count = os.cpu_count() or 1
    memory_gib: float | None = None
    try:
        if platform.system() == "Darwin":
            memory_bytes = int(
                subprocess.run(
                    ["sysctl", "-n", "hw.memsize"],
                    check=True,
                    capture_output=True,
                    text=True,
                ).stdout
            )
        else:
            memory_bytes = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
        memory_gib = memory_bytes / 1024**3
    except (AttributeError, OSError, ValueError, subprocess.SubprocessError):
        memory_gib = None
    docker_cli = shutil.which("docker") is not None
    docker_daemon = False
    if docker_cli:
        try:
            docker_daemon = (
                subprocess.run(
                    ["docker", "info"],
                    check=False,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=5,
                ).returncode
                == 0
            )
        except subprocess.TimeoutExpired:
            docker_daemon = False
    resources_ready = cpu_count >= 2 and free_gib >= 5 and (memory_gib is None or memory_gib >= 6)
    local_ready = (3, 11) <= sys.version_info[:2] < (3, 13) and resources_ready and docker_daemon
    return {
        "profile": "local-standard" if local_ready else "browser-fallback",
        "local_ready": local_ready,
        "python": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
        "docker_cli": docker_cli,
        "docker_daemon": docker_daemon,
        "cpu_count": cpu_count,
        "memory_gib": round(memory_gib, 1) if memory_gib is not None else None,
        "disk_free_gib": round(free_gib, 1),
        "next": "continue locally" if local_ready else "use the prepared browser workspace",
    }
