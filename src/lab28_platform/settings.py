"""Environment-driven configuration for every platform boundary.

Nothing in this module reads a secret from a file in the repository. Secrets
arrive as environment variables and are never logged or serialised.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from lab28_platform.contracts import (
    QDRANT_COLLECTION,
    TOPIC_DATA_PROCESSED,
    TOPIC_DATA_RAW,
    TOPIC_DATA_RAW_DLQ,
    TOPIC_MODEL_EVENTS,
)


def _load_env_files() -> None:
    for path in (Path(".env"), Path("/tmp/lab28-ports.local")):
        if path.is_file():
            try:
                for line in path.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        k, v = k.strip(), v.strip().strip("'\"")
                        if k not in os.environ:
                            os.environ[k] = v
            except Exception:
                pass


_load_env_files()


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return int(raw) if raw else default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    return float(raw) if raw else default


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str
    topic_raw: str
    topic_processed: str
    topic_model_events: str
    topic_dlq: str
    group_id: str
    max_delivery_attempts: int
    delivery_timeout_seconds: float

    @classmethod
    def from_env(cls) -> KafkaSettings:
        return cls(
            bootstrap_servers=_env("LAB28_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            topic_raw=_env("LAB28_TOPIC_RAW", TOPIC_DATA_RAW),
            topic_processed=_env("LAB28_TOPIC_PROCESSED", TOPIC_DATA_PROCESSED),
            topic_model_events=_env("LAB28_TOPIC_MODEL_EVENTS", TOPIC_MODEL_EVENTS),
            topic_dlq=_env("LAB28_TOPIC_DLQ", TOPIC_DATA_RAW_DLQ),
            group_id=_env("LAB28_KAFKA_GROUP_ID", "lab28-pipeline"),
            max_delivery_attempts=_env_int("LAB28_KAFKA_MAX_ATTEMPTS", 3),
            delivery_timeout_seconds=_env_float("LAB28_KAFKA_DELIVERY_TIMEOUT", 10.0),
        )


#: Dense embedding model for the vector store.
#:
#: Chosen from the four multilingual models fastembed 0.8.0 actually ships. It
#: is the only *small* one (384 dimensions, 0.22 GB), it is Apache-2.0 so
#: students may reuse the lab commercially, and it covers Vietnamese. Note that
#: fastembed has no multilingual-e5-small or -base — the only e5 multilingual
#: option is the 2.24 GB large variant, which is too heavy to bake into an image.
DEFAULT_EMBEDDING_MODEL = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
DEFAULT_EMBEDDING_DIM = 384

#: Commit SHA of the HuggingFace repo fastembed actually downloads for the model
#: above (an INT8-quantized ONNX conversion, not the sentence-transformers repo).
#: Pinning the revision is what makes an embedding reproducible across rebuilds.
DEFAULT_EMBEDDING_REVISION = "faf4aa4225822f3bc6376869cb1164e8e3feedd0"

#: Language-agnostic sparse model for the lexical half of hybrid retrieval.
DEFAULT_SPARSE_MODEL = "Qdrant/bm25"
DEFAULT_SPARSE_REVISION = "22b8d2af71a76161e18dd432d2cee0eefa66e412"


@dataclass(frozen=True)
class QdrantSettings:
    url: str
    api_key_env: str
    collection: str
    timeout_seconds: float
    embedding_model: str
    embedding_revision: str
    embedding_dim: int
    sparse_model: str
    sparse_revision: str
    cache_dir: str | None
    prefetch_limit: int

    @classmethod
    def from_env(cls) -> QdrantSettings:
        return cls(
            url=_env("LAB28_QDRANT_URL", "http://localhost:6333"),
            api_key_env="LAB28_QDRANT_API_KEY",
            collection=_env("LAB28_QDRANT_COLLECTION", QDRANT_COLLECTION),
            timeout_seconds=_env_float("LAB28_QDRANT_TIMEOUT", 5.0),
            # Pinned by name and revision so the same text always yields the
            # same vector, and so provenance can be recorded in MLflow.
            embedding_model=_env("LAB28_EMBEDDING_MODEL", DEFAULT_EMBEDDING_MODEL),
            embedding_revision=_env("LAB28_EMBEDDING_REVISION", DEFAULT_EMBEDDING_REVISION),
            embedding_dim=_env_int("LAB28_EMBEDDING_DIM", DEFAULT_EMBEDDING_DIM),
            sparse_model=_env("LAB28_SPARSE_MODEL", DEFAULT_SPARSE_MODEL),
            sparse_revision=_env("LAB28_SPARSE_REVISION", DEFAULT_SPARSE_REVISION),
            cache_dir=os.getenv("FASTEMBED_CACHE_PATH"),
            # Each hybrid branch retrieves this many candidates before fusion.
            prefetch_limit=_env_int("LAB28_PREFETCH_LIMIT", 20),
        )

    @property
    def api_key(self) -> str | None:
        """Read at call time; never cached or serialised."""
        return os.getenv(self.api_key_env)

    @property
    def embedding_model_id(self) -> str:
        """Provenance string recorded in MLflow and in the serving evidence."""
        return f"{self.embedding_model}@{self.embedding_revision}"


@dataclass(frozen=True)
class FeastSettings:
    """Feast is reached over HTTP only; the serving image never imports it.

    ``metrics_url`` is a second port on purpose: Feast serves Prometheus from a
    separate server on 8000, and ``/metrics`` on the feature-server port is a
    404. Scraping the wrong one is the usual reason IP04 shows no data.
    """

    repo_path: Path
    server_url: str
    metrics_url: str
    timeout_seconds: float

    @classmethod
    def from_env(cls) -> FeastSettings:
        return cls(
            repo_path=Path(_env("LAB28_FEAST_REPO", "feature-repo")).expanduser(),
            server_url=_env("LAB28_FEAST_SERVER_URL", "http://localhost:6566"),
            metrics_url=_env("LAB28_FEAST_METRICS_URL", "http://localhost:6570"),
            timeout_seconds=_env_float("LAB28_FEAST_TIMEOUT", 3.0),
        )


@dataclass(frozen=True)
class MLflowSettings:
    tracking_uri: str
    model_name: str
    alias: str
    experiment: str
    timeout_seconds: float

    @classmethod
    def from_env(cls) -> MLflowSettings:
        mlflow_port = _env("LAB28_MLFLOW_PORT", "5000")
        return cls(
            tracking_uri=_env("MLFLOW_TRACKING_URI", f"http://localhost:{mlflow_port}"),
            model_name=_env("LAB28_MODEL_NAME", "lab28-rag-release"),
            alias=_env("LAB28_MODEL_ALIAS", "champion"),
            experiment=_env("LAB28_MLFLOW_EXPERIMENT", "lab28-platform"),
            timeout_seconds=_env_float("LAB28_MLFLOW_TIMEOUT", 5.0),
        )


@dataclass(frozen=True)
class VLLMSettings:
    """Configuration for the real vLLM OpenAI-compatible endpoint.

    ``require_real`` makes the live gate refuse an endpoint that cannot prove it
    is vLLM. It is on by default so a mock can never satisfy the gate.
    """

    base_url: str
    model_id: str
    api_key_env: str
    timeout_seconds: float
    max_tokens: int
    temperature: float
    require_real: bool

    @classmethod
    def from_env(cls) -> VLLMSettings:
        return cls(
            base_url=_env("LAB28_VLLM_BASE_URL", "http://localhost:8001/v1"),
            model_id=_env("LAB28_VLLM_MODEL_ID", "Qwen/Qwen3-1.7B"),
            api_key_env="LAB28_VLLM_API_KEY",
            timeout_seconds=_env_float("LAB28_VLLM_TIMEOUT", 30.0),
            max_tokens=_env_int("LAB28_VLLM_MAX_TOKENS", 320),
            temperature=_env_float("LAB28_VLLM_TEMPERATURE", 0.2),
            require_real=_env_flag("LAB28_VLLM_REQUIRE_REAL", True),
        )

    @property
    def api_key(self) -> str | None:
        """Read the key at call time; never cache or serialise it."""
        return os.getenv(self.api_key_env)

    @property
    def root_url(self) -> str:
        return self.base_url.removesuffix("/").removesuffix("/v1")


@dataclass(frozen=True)
class TelemetrySettings:
    service_name: str
    otlp_endpoint: str
    enabled: bool
    console_export: bool
    sample_ratio: float

    @classmethod
    def from_env(cls, service_name: str = "lab28-api") -> TelemetrySettings:
        return cls(
            service_name=_env("OTEL_SERVICE_NAME", service_name),
            otlp_endpoint=_env("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317"),
            enabled=_env_flag("LAB28_OTEL_ENABLED", True),
            console_export=_env_flag("LAB28_TRACE_CONSOLE", False),
            sample_ratio=_env_float("LAB28_TRACE_SAMPLE_RATIO", 1.0),
        )


@dataclass(frozen=True)
class ServingSettings:
    """Latency budgets from the slide's request audit trail."""

    feature_budget_ms: float
    retrieval_budget_ms: float
    llm_budget_ms: float
    total_budget_ms: float
    allow_degraded: bool

    @classmethod
    def from_env(cls) -> ServingSettings:
        return cls(
            feature_budget_ms=_env_float("LAB28_BUDGET_FEATURE_MS", 5.0),
            retrieval_budget_ms=_env_float("LAB28_BUDGET_RETRIEVAL_MS", 50.0),
            llm_budget_ms=_env_float("LAB28_BUDGET_LLM_MS", 500.0),
            total_budget_ms=_env_float("LAB28_BUDGET_TOTAL_MS", 1000.0),
            allow_degraded=_env_flag("LAB28_ALLOW_DEGRADED", True),
        )


@dataclass(frozen=True)
class Settings:
    """Aggregate settings handed to the API, the CLI and every job."""

    runtime_dir: Path

    #: Root of the Delta tables, as a path *relative to the repository root*.
    #: Relative on purpose: the same string has to resolve to the same table
    #: from three places — the host (tests and the CLI read Delta natively with
    #: ``deltalake``), the Spark Connect server, and the Airflow task — and the
    #: only way one absolute path works everywhere is if every machine agrees on
    #: it. macOS will not let anyone create ``/data``, so the containers bind
    #: the repository at a fixed ``working_dir`` instead and let the relative
    #: path do the agreeing.
    delta_root: str

    gateway_url: str
    api_url: str

    #: Where short-lived jobs push their metrics. Airflow tasks and Spark jobs
    #: exit long before Prometheus comes round to scrape them, so IP02/IP03
    #: metrics reach the dashboards through the Pushgateway instead.
    pushgateway_url: str

    #: Spark Connect endpoint used by the Delta writer. ``sc://`` is Connect's
    #: own gRPC scheme, not a ``spark://`` cluster-manager address.
    spark_remote: str

    kafka: KafkaSettings = field(default_factory=KafkaSettings.from_env)
    qdrant: QdrantSettings = field(default_factory=QdrantSettings.from_env)
    feast: FeastSettings = field(default_factory=FeastSettings.from_env)
    mlflow: MLflowSettings = field(default_factory=MLflowSettings.from_env)
    vllm: VLLMSettings = field(default_factory=VLLMSettings.from_env)
    telemetry: TelemetrySettings = field(default_factory=TelemetrySettings.from_env)
    serving: ServingSettings = field(default_factory=ServingSettings.from_env)

    @classmethod
    def from_env(cls) -> Settings:
        gateway_port = _env("LAB28_GATEWAY_PORT", "8080")
        api_port = _env("LAB28_API_PORT", "8000")
        return cls(
            runtime_dir=Path(_env("LAB28_RUNTIME_DIR", ".lab28")).expanduser().resolve(),
            delta_root=_env("LAB28_DELTA_ROOT", ".lab28/delta"),
            gateway_url=_env("LAB28_GATEWAY_URL", f"http://localhost:{gateway_port}"),
            api_url=_env("LAB28_API_URL", f"http://localhost:{api_port}"),
            pushgateway_url=_env("LAB28_PUSHGATEWAY_URL", "http://localhost:9091"),
            spark_remote=_env("LAB28_SPARK_REMOTE", "sc://localhost:15002"),
            kafka=KafkaSettings.from_env(),
            qdrant=QdrantSettings.from_env(),
            feast=FeastSettings.from_env(),
            mlflow=MLflowSettings.from_env(),
            vllm=VLLMSettings.from_env(),
            telemetry=TelemetrySettings.from_env(),
            serving=ServingSettings.from_env(),
        )

    @property
    def feedback_table(self) -> str:
        return f"{self.delta_root.rstrip('/')}/feedback"

    @property
    def document_table(self) -> str:
        return f"{self.delta_root.rstrip('/')}/documents"

    @property
    def asker_features_path(self) -> str:
        """Delta-derived offline feature snapshot that Feast reads."""
        return f"{self.delta_root.rstrip('/')}/exports/asker_activity"
