"""``lab28`` — the operator entry point for the platform.

Every command answers one question a student or an operator actually asks:
*is my machine capable*, *do the topics exist*, *is there anything to retrieve*,
*what is serving right now*, *can I roll back*, *what can I prove*.

Two conventions run through the whole file.

**stdout is JSON, stderr is prose.** ``lab28 inspect > state.json`` produces a
usable artefact, and the running commentary a human wants does not corrupt it.

**Heavy imports live inside commands.** ``lab28 preflight`` is the first command
a student runs — before ``uv sync --extra ml``, sometimes before Docker is up —
and its entire job is to say whether this machine can run the stack. Importing
mlflow or qdrant-client at module scope would make it fail exactly when it is
most needed. Everything below the base dependency set is therefore imported
where it is used, not at the top.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Annotated, Any

import typer

from lab28_platform.settings import Settings

app = typer.Typer(
    no_args_is_help=True,
    add_completion=False,
    help="Operate the lab28 platform: ingest, index, release, prove.",
)

#: Prompt used by ``lab28 release`` unless ``--template`` points elsewhere.
#: It uses both placeholders so the rendered prompt is fully determined by the
#: template rather than by the pipeline's append-what-is-missing fallback.
DEFAULT_PROMPT_TEMPLATE = (
    "NGỮ CẢNH:\n{context}\n\n"
    "CÂU HỎI: {question}\n\n"
    "Trả lời ngắn gọn bằng tiếng Việt, chỉ dựa trên ngữ cảnh ở trên. "
    "Trích dẫn số thứ tự của tài liệu bạn đã dùng, ví dụ [1]. "
    "Nếu ngữ cảnh không chứa câu trả lời, hãy nói rõ là bạn không biết."
)

DATA_DIR = Path("data")
EVIDENCE_DIR = Path("evidence")


def _emit(payload: Any) -> None:
    """Write one JSON document to stdout."""
    json.dump(payload, sys.stdout, indent=2, ensure_ascii=False, default=str)
    sys.stdout.write("\n")


def _note(message: str) -> None:
    """Write commentary to stderr so stdout stays machine-readable."""
    typer.secho(message, err=True, fg=typer.colors.CYAN)


def _fail(message: str) -> None:
    typer.secho(message, err=True, fg=typer.colors.RED)
    raise typer.Exit(code=1)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        _fail(f"{path} not found")
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


# --------------------------------------------------------------------------
# Environment
# --------------------------------------------------------------------------


@app.command()
def preflight() -> None:
    """Report whether this machine can run the stack locally."""
    from lab28_platform import readiness

    report = readiness.run_preflight()
    _note(f"profile: {report['profile']} — {report['next']}")
    _emit(report)


def _generated_path(path: Path, repository: Path) -> Path:
    """Resolve a generated path and refuse broad or external deletion targets."""
    resolved = path.expanduser().resolve()
    root = repository.resolve()
    if resolved == root or root not in resolved.parents:
        _fail(f"refusing to delete {resolved}; generated state must be inside {root}")
    return resolved


def _remove_generated(path: Path, *, keep: frozenset[str] = frozenset()) -> list[str]:
    """Delete generated children without shell commands; works on every host OS."""
    if not path.exists():
        return []
    removed: list[str] = []
    for child in path.iterdir():
        if child.name in keep:
            continue
        if child.is_dir() and not child.is_symlink():
            shutil.rmtree(child)
        else:
            child.unlink()
        removed.append(str(child))
    if not keep and path.exists():
        path.rmdir()
    return removed


@app.command()
def reset(
    yes: Annotated[
        bool, typer.Option("--yes", help="Confirm deletion of generated state.")
    ] = False,
    containers: Annotated[
        bool,
        typer.Option(help="Stop Compose services and remove their named volumes."),
    ] = True,
    keep_models: Annotated[
        bool,
        typer.Option(help="Keep the downloaded FastEmbed model cache."),
    ] = True,
) -> None:
    """Reset the lab safely on Windows, macOS or Linux.

    Only repository-local generated state is removed. Source data and code are
    never targets, and no Bash/PowerShell-specific command is used.
    """
    if not yes:
        _fail("reset deletes generated lab state; run again with --yes")

    repository = Path.cwd().resolve()
    settings = Settings.from_env()
    runtime = _generated_path(settings.runtime_dir, repository)
    evidence = _generated_path(repository / EVIDENCE_DIR, repository)

    compose_result: dict[str, Any] = {"requested": containers, "stopped": False}
    if containers:
        if shutil.which("docker") is None:
            _fail("docker is not available; use --no-containers to reset local files only")
        completed = subprocess.run(
            ["docker", "compose", "down", "--volumes", "--remove-orphans"],
            cwd=repository,
            capture_output=True,
            text=True,
            check=False,
        )
        compose_result.update(
            {
                "stopped": completed.returncode == 0,
                "returncode": completed.returncode,
                "detail": (completed.stderr or completed.stdout).strip()[-1000:],
            }
        )
        if completed.returncode != 0:
            _emit({"compose": compose_result, "removed": []})
            _fail("docker compose down failed; local state was left untouched")

    kept = frozenset({"fastembed"}) if keep_models else frozenset()
    removed = _remove_generated(runtime, keep=kept)
    removed.extend(_remove_generated(evidence))
    _emit(
        {
            "compose": compose_result,
            "removed": removed,
            "kept": [str(runtime / name) for name in sorted(kept) if (runtime / name).exists()],
        }
    )


# --------------------------------------------------------------------------
# IP01 — Kafka
# --------------------------------------------------------------------------


@app.command()
def topics() -> None:
    """Create the declared Kafka topics with their retention policy."""
    from lab28_platform.event_bus import broker_metadata, ensure_topics

    settings = Settings.from_env()
    results = ensure_topics(settings.kafka)
    metadata = broker_metadata(settings.kafka)
    _note(f"{metadata['brokers']} broker(s) at {settings.kafka.bootstrap_servers}")
    _emit({"topics": results, "broker_topics": metadata["topics"]})


@app.command()
def seed(
    limit: Annotated[int, typer.Option(help="Maximum records of each kind to send.")] = 0,
    via_gateway: Annotated[
        bool, typer.Option(help="Send through the gateway instead of the API directly.")
    ] = False,
) -> None:
    """Submit the bundled corpus through the real ingestion endpoints.

    Seeding goes over HTTP rather than straight to Kafka on purpose: the API is
    the only sanctioned producer, so seeding this way exercises validation, the
    idempotency key and the traceparent header exactly as a real client would.
    """
    import time

    import httpx

    settings = Settings.from_env()
    base = (settings.gateway_url if via_gateway else settings.api_url).rstrip("/")
    batches = {
        "documents": _read_jsonl(DATA_DIR / "documents.jsonl"),
        "feedback": _read_jsonl(DATA_DIR / "feedback.jsonl"),
    }

    accepted: dict[str, list[dict[str, Any]]] = {}
    rejected: dict[str, list[dict[str, Any]]] = {}
    with httpx.Client(base_url=base, timeout=15.0) as client:
        for kind, rows in batches.items():
            selected = rows[:limit] if limit else rows
            accepted[kind], rejected[kind] = [], []
            for row in selected:
                for attempt in range(3):
                    response = client.post(f"/api/v1/{kind}", json=row)
                    if response.status_code != 429 or attempt == 2:
                        break
                    # The bundled corpus is larger than Envoy's one-second
                    # token bucket. Wait for the next refill and retry only
                    # requests that never reached the API.
                    time.sleep(1.0)
                target = accepted if response.status_code == 202 else rejected
                target[kind].append(
                    response.json()
                    if response.headers.get("content-type", "").startswith("application/json")
                    else {"status_code": response.status_code, "body": response.text[:200]}
                )
            _note(f"{kind}: {len(accepted[kind])} accepted, {len(rejected[kind])} rejected")

    _emit({"target": base, "accepted": accepted, "rejected": rejected})
    if any(rejected.values()):
        raise typer.Exit(code=1)


@app.command()
def dlq(
    replay: Annotated[bool, typer.Option(help="Re-publish dead letters to the source topic.")] = (
        False
    ),
    limit: Annotated[int, typer.Option(help="Maximum dead letters to read or replay.")] = 20,
) -> None:
    """Inspect the dead-letter topic, and optionally replay it.

    Replay is an operator action, never automatic: a message that was
    dead-lettered by a defect will loop straight back until the defect is fixed.
    """
    from lab28_platform.event_bus import dead_letter_count, decode_dead_letters, replay_dead_letters

    settings = Settings.from_env()
    if replay:
        _note("replaying dead letters — fix the defect first, or they will return")
        _emit(replay_dead_letters(settings.kafka, limit=limit))
        return
    _emit(
        {
            "topic": settings.kafka.topic_dlq,
            "count": dead_letter_count(settings.kafka),
            "sample": decode_dead_letters(settings.kafka, limit=limit),
        }
    )


# --------------------------------------------------------------------------
# IP05 — Qdrant
# --------------------------------------------------------------------------


@app.command()
def index(
    source: Annotated[
        str, typer.Option(help="'delta' (the lakehouse) or 'file' (the bundled corpus).")
    ] = "delta",
    limit: Annotated[int, typer.Option(help="Maximum documents to index.")] = 0,
) -> None:
    """Embed and index documents into the hybrid Qdrant collection.

    ``--source file`` exists so retrieval can be demonstrated before Airflow and
    Spark have run, but it bypasses the lakehouse — the source used is always
    reported so a green demo cannot quietly hide a missing Delta table.
    """
    from lab28_platform.vector_store import VectorStore, documents_from_rows

    settings = Settings.from_env()
    if source == "delta":
        from lab28_platform import delta_store

        rows = delta_store.read_rows(settings.document_table, limit=limit or None)
    elif source == "file":
        rows = _read_jsonl(DATA_DIR / "documents.jsonl")
        rows = rows[:limit] if limit else rows
    else:
        _fail(f"unknown source {source!r}; expected 'delta' or 'file'")

    store = VectorStore(settings.qdrant)
    try:
        documents = documents_from_rows(rows)
        _note(f"embedding {len(documents)} document(s) from {source}")
        indexed = store.index(documents)
        _emit(
            {
                "source": source,
                "collection": store.collection,
                "documents_submitted": len(documents),
                "points_upserted": indexed,
                "points_total": store.count(),
                "embedding_model_id": settings.qdrant.embedding_model_id,
            }
        )
    finally:
        store.close()


# --------------------------------------------------------------------------
# IP06 — MLflow releases
# --------------------------------------------------------------------------


@app.command()
def release(
    prompt_version: Annotated[str, typer.Option(help="Version label for this prompt.")] = "v1",
    template: Annotated[
        Path | None, typer.Option(help="File holding the prompt template.")
    ] = None,
    top_k: Annotated[int, typer.Option(help="Documents retrieved per question.")] = 3,
    promote: Annotated[bool, typer.Option(help="Point the champion alias at it.")] = True,
) -> None:
    """Register a serving release and, by default, promote it.

    A release is prompt plus retrieval configuration plus served model id plus
    the data version it was evaluated against — that bundle is what rollback
    needs to be able to restore.
    """
    from lab28_platform.contracts import FEATURE_SERVICE_NAME
    from lab28_platform.model_registry import ReleaseRegistry, ReleaseSpec

    settings = Settings.from_env()
    spec = ReleaseSpec(
        prompt_version=prompt_version,
        prompt_template=(
            template.read_text(encoding="utf-8") if template else DEFAULT_PROMPT_TEMPLATE
        ),
        vllm_model_id=settings.vllm.model_id,
        embedding_model_id=settings.qdrant.embedding_model_id,
        qdrant_collection=settings.qdrant.collection,
        feature_service=FEATURE_SERVICE_NAME,
        top_k=top_k,
        delta_version=_delta_version(settings.feedback_table),
    )
    registry = ReleaseRegistry(settings.mlflow)
    entry = registry.register(spec, promote=promote)
    _note(f"registered {entry.name} v{entry.version}" + (" as champion" if promote else ""))
    _emit(entry.to_dict())


@app.command()
def rollback() -> None:
    """Move the champion alias to the previous version."""
    from lab28_platform.model_registry import RegistryUnavailable, ReleaseRegistry

    registry = ReleaseRegistry(Settings.from_env().mlflow)
    try:
        previous = registry.current_version()
        entry = registry.rollback()
    except RegistryUnavailable as error:
        # Nothing promoted, or only one version exists. That is an operator
        # mistake, not a crash — say what is missing and exit non-zero.
        _fail(str(error))
    _note(f"champion moved from v{previous} to v{entry.version}")
    _emit(entry.to_dict())


def _delta_version(uri: str) -> int | None:
    """The current Delta version, or None when the lakehouse is not up yet."""
    try:
        from lab28_platform import delta_store

        return delta_store.current_version(uri)
    except Exception:
        return None


# --------------------------------------------------------------------------
# Serving
# --------------------------------------------------------------------------


@app.command()
def ask(
    question: Annotated[str, typer.Argument(help="The question to answer.")],
    asker_id: Annotated[str, typer.Option(help="Entity id used for the feature lookup.")] = "demo",
    top_k: Annotated[int, typer.Option(help="Documents to retrieve.")] = 3,
    via_gateway: Annotated[bool, typer.Option(help="Go through the gateway.")] = False,
) -> None:
    """Ask one question over HTTP and print the full answer with its evidence."""
    import httpx

    settings = Settings.from_env()
    base = (settings.gateway_url if via_gateway else settings.api_url).rstrip("/")
    response = httpx.post(
        f"{base}/api/v1/ask",
        json={"asker_id": asker_id, "question": question, "top_k": top_k},
        timeout=settings.serving.total_budget_ms / 1000 * 60,
    )
    body = response.json()
    if response.status_code != 200:
        _emit(body)
        _fail(f"{response.status_code} {body.get('category', 'error')}: {body.get('message', '')}")

    evidence = body["evidence"]
    _note(f"trace {evidence['trace_id']} — {body['audit']['latency']['total_ms']} ms")
    if evidence["degraded"]:
        _note("degraded: " + "; ".join(evidence["degraded_reasons"]))
    _emit(body)


@app.command()
def serve(
    host: Annotated[str, typer.Option(help="Bind address.")] = "0.0.0.0",
    port: Annotated[int, typer.Option(help="Bind port.")] = 8000,
) -> None:
    """Run the API with uvicorn."""
    import uvicorn

    uvicorn.run(
        "lab28_platform.api:create_app",
        factory=True,
        host=host,
        port=port,
        log_level="info",
    )


# --------------------------------------------------------------------------
# Proof
# --------------------------------------------------------------------------


@app.command()
def inspect() -> None:
    """Snapshot every dependency the platform claims to integrate.

    Each probe is isolated: one unreachable dependency must not blank out the
    report, because the report is most useful precisely when something is down.
    """
    settings = Settings.from_env()
    report: dict[str, Any] = {}

    def attempt(name: str, thunk: Any) -> None:
        try:
            report[name] = thunk()
        except Exception as error:
            report[name] = {"error": f"{type(error).__name__}: {error}"}

    def kafka() -> Any:
        from lab28_platform.event_bus import broker_metadata

        return broker_metadata(settings.kafka)

    def delta() -> Any:
        from lab28_platform import delta_store

        return delta_store.health(
            {"feedback": settings.feedback_table, "documents": settings.document_table}
        )

    def feast() -> Any:
        from lab28_platform.feature_store import FeatureClient

        client = FeatureClient(settings.feast)
        try:
            return client.health()
        finally:
            client.close()

    def qdrant() -> Any:
        from lab28_platform.vector_store import VectorStore

        store = VectorStore(settings.qdrant)
        try:
            return store.health()
        finally:
            store.close()

    def mlflow() -> Any:
        from lab28_platform.model_registry import ReleaseRegistry

        return ReleaseRegistry(settings.mlflow).health()

    def vllm() -> Any:
        from lab28_platform.llm_client import probe_identity

        return probe_identity(settings.vllm).to_dict()

    for name, thunk in (
        ("kafka", kafka),
        ("spark-delta", delta),
        ("feast", feast),
        ("qdrant", qdrant),
        ("mlflow", mlflow),
        ("vllm", vllm),
    ):
        attempt(name, thunk)
        _note(f"probed {name}")

    _emit(report)


@app.command()
def ready() -> None:
    """Serving readiness — the same verdict the ``/ready`` endpoint returns."""
    from lab28_platform import readiness

    report = readiness.serving_readiness(Settings.from_env())
    _note(f"status: {report.status}")
    _emit(report.model_dump(mode="json"))
    if report.status == "not_ready":
        raise typer.Exit(code=1)


@app.command()
def integration() -> None:
    """Score the ten integration points against the matrix. Exits 1 if not ready."""
    from lab28_platform import readiness

    report = readiness.integration_report(Settings.from_env())
    for point in report["points"]:
        _note(f"{point['id']} {point['status']:<10} {point['name']}")
    _note(
        f"{report['passing_points']}/{report['verified_points']} verified points passing; "
        f"{report['unverified_points']} not provable from this process"
    )
    _emit(report)
    if not report["ready"]:
        raise typer.Exit(code=1)


@app.command()
def evidence(
    out: Annotated[Path, typer.Option(help="Directory to write evidence files into.")] = (
        EVIDENCE_DIR
    ),
    question: Annotated[str, typer.Option(help="Query used for the retrieval evidence.")] = (
        "Nền tảng dữ liệu của lab này gồm những thành phần nào?"
    ),
) -> None:
    """Write the evidence files this process can genuinely produce.

    Only four integration points can be proved from inside the CLI. The others
    need an Airflow run, a live gateway, a Prometheus scrape or a trace backend,
    so they are listed as outstanding rather than written as empty files — an
    evidence directory that looks complete but is not is worse than one that is
    honestly short.
    """
    from lab28_platform import delta_store
    from lab28_platform.llm_client import probe_identity
    from lab28_platform.model_registry import ReleaseRegistry
    from lab28_platform.vector_store import VectorStore

    settings = Settings.from_env()
    out.mkdir(parents=True, exist_ok=True)
    written: dict[str, str] = {}
    failed: dict[str, str] = {}

    def write(filename: str, thunk: Any) -> None:
        try:
            payload = thunk()
        except Exception as error:
            failed[filename] = f"{type(error).__name__}: {error}"
            _note(f"skipped {filename}: {failed[filename]}")
            return
        path = out / filename
        path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8"
        )
        written[filename] = str(path)
        _note(f"wrote {path}")

    def delta_history() -> Any:
        return {
            "feedback": {
                "history": delta_store.commit_history(settings.feedback_table),
                "time_travel": delta_store.time_travel_evidence(
                    "feedback", settings.feedback_table
                ),
            },
            "documents": {
                "history": delta_store.commit_history(settings.document_table),
            },
        }

    def qdrant_search() -> Any:
        import gc

        from lab28_platform.vector_store import release_embedder_cache

        store: VectorStore | None = VectorStore(settings.qdrant)
        try:
            sources = store.search(question, top_k=5)
            result = {
                "collection": store.collection,
                "question": question,
                "points_total": store.count(),
                "embedding_model_id": settings.qdrant.embedding_model_id,
                "results": [source.model_dump(mode="json") for source in sources],
            }
        finally:
            if store is not None:
                store.close()
            store = None
            release_embedder_cache()
            # FastEmbed owns native ONNX sessions. Reclaim them while Python's
            # runtime is still alive instead of during interpreter teardown.
            gc.collect()
        return result

    write("ip03-delta-history.json", delta_history)
    write("ip05-qdrant-search.json", qdrant_search)
    write("ip06-mlflow-release.json", lambda: ReleaseRegistry(settings.mlflow).health())
    write("ip07-vllm-identity.json", lambda: probe_identity(settings.vllm).to_dict())

    from lab28_platform import readiness

    write("integration-report.json", lambda: readiness.integration_report(settings))

    outstanding = {
        "ip01-kafka-consume.json": "integration test: consume from data.raw and keep the headers",
        "ip02-airflow-run.json": "Airflow: DAG run id, task states, asset event",
        "ip04-feast-online.json": "Feast: online row after materialization",
        "ip08-gateway.json": "gateway: a 200 and a 429 with x-request-id",
        "ip09-prometheus-targets.json": "Prometheus: targets and alert rules",
        "ip10-trace.json": "trace backend: one trace carrying the required span names",
    }
    _note(f"{len(outstanding)} evidence file(s) must come from outside this process")
    _emit({"written": written, "failed": failed, "outstanding": outstanding})
    sys.stdout.flush()
    sys.stderr.flush()
    import os

    os._exit(0)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
