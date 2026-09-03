"""Qdrant hybrid retrieval (IP05).

Hybrid means two retrievers over the same collection — a dense vector for
semantic similarity and a BM25 sparse vector for exact lexical matching — fused
server-side with Reciprocal Rank Fusion. Vietnamese questions frequently hinge
on a proper noun the dense model smooths away, which is exactly what the sparse
branch recovers.

Three details are load-bearing and easy to get wrong:

* Point IDs must be a uint64 or a UUID; Qdrant rejects arbitrary strings. IDs
  are derived from the document ID with UUIDv5 so re-indexing overwrites the
  same point instead of duplicating it.
* ``Modifier.IDF`` on the sparse vector config is mandatory for ``Qdrant/bm25``.
  fastembed emits raw term weights and the server applies IDF; omit it and
  ranking is silently wrong with no error.
* An upsert replaces the entire point, so both named vectors are always written
  together. Sending only one would null out the other.
"""

from __future__ import annotations

import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

from qdrant_client import QdrantClient, models

from lab28_platform import metrics
from lab28_platform.contracts import (
    DENSE_VECTOR_NAME,
    SPARSE_VECTOR_NAME,
    RetrievedSource,
    stable_point_id,
)
from lab28_platform.settings import QdrantSettings
from lab28_platform.telemetry import SPAN_QDRANT_QUERY, span

SNIPPET_CHARS = 400


class RetrievalUnavailable(RuntimeError):
    """Qdrant is unreachable, or the collection is missing."""


@dataclass(frozen=True)
class IndexableDocument:
    """One document to embed and index."""

    doc_id: str
    title: str
    text: str
    locale: str = "vi"
    tags: tuple[str, ...] = ()


class Embedder:
    """fastembed dense + sparse encoders, pinned by name and revision.

    Loaded lazily and cached per process: constructing the ONNX sessions costs
    seconds, and the API would otherwise pay that on its first request.
    """

    def __init__(self, settings: QdrantSettings) -> None:
        self._settings = settings
        self._dense: Any = None
        self._sparse: Any = None

    def _load(self) -> None:
        if self._dense is not None:
            return
        from fastembed import SparseTextEmbedding, TextEmbedding

        kwargs: dict[str, Any] = {}
        if self._settings.cache_dir:
            kwargs["cache_dir"] = self._settings.cache_dir

        # fastembed 0.8.0 resolves the commit itself and ignores a ``revision``
        # kwarg, so the pin is enforced after the fact by verify_model_pins()
        # against the SHA it actually materialised on disk.
        self._dense = TextEmbedding(model_name=self._settings.embedding_model, **kwargs)
        self._sparse = SparseTextEmbedding(
            model_name=self._settings.sparse_model, **kwargs
        )

    def embed_passages(self, texts: Sequence[str]) -> tuple[list[list[float]], list[Any]]:
        """Encode documents. Uses the model's passage prefix where it has one."""
        self._load()
        started = time.perf_counter()
        dense = [vector.tolist() for vector in self._dense.passage_embed(list(texts))]
        sparse = list(self._sparse.embed(list(texts)))
        metrics.EMBEDDING_SECONDS.labels(stage="passage").observe(
            time.perf_counter() - started
        )
        return dense, sparse

    def embed_query(self, text: str) -> tuple[list[float], Any]:
        """Encode a query. Uses the model's query prefix where it has one."""
        self._load()
        started = time.perf_counter()
        dense = next(iter(self._dense.query_embed(text))).tolist()
        sparse = next(iter(self._sparse.query_embed(text)))
        metrics.EMBEDDING_SECONDS.labels(stage="query").observe(
            time.perf_counter() - started
        )
        return dense, sparse

    @property
    def dimension(self) -> int:
        return self._settings.embedding_dim


@lru_cache(maxsize=4)
def _shared_embedder(cache_key: str, settings: QdrantSettings) -> Embedder:
    del cache_key
    return Embedder(settings)


def get_embedder(settings: QdrantSettings) -> Embedder:
    """Process-wide embedder for the given model pin."""
    return _shared_embedder(settings.embedding_model_id, settings)


def release_embedder_cache() -> None:
    """Release cached ONNX sessions before a short-lived CLI process exits."""
    _shared_embedder.cache_clear()


def source_repo(model_name: str, *, sparse: bool = False) -> str:
    """The HuggingFace repo fastembed downloads for a friendly model name.

    They are not the same string: the dense model above is published by
    sentence-transformers but fastembed fetches Qdrant's quantised ONNX
    conversion of it. Evidence has to name the repo that was really used.
    """
    from fastembed import SparseTextEmbedding, TextEmbedding

    cls = SparseTextEmbedding if sparse else TextEmbedding
    for description in cls._list_supported_models():
        if description.model == model_name:
            return description.sources.hf or model_name
    raise LookupError(f"fastembed does not ship a model named {model_name!r}")


def resolve_pinned_revisions(settings: QdrantSettings) -> dict[str, dict[str, Any]]:
    """Report which commit of each model is actually present in the cache.

    fastembed lays the cache out like the HuggingFace hub —
    ``models--<org>--<repo>/snapshots/<commit>`` — so the commit that will be
    loaded is readable from disk without a network call.
    """
    from pathlib import Path

    root = Path(settings.cache_dir) if settings.cache_dir else None
    report: dict[str, dict[str, Any]] = {}
    for label, name, expected, sparse in (
        ("dense", settings.embedding_model, settings.embedding_revision, False),
        ("sparse", settings.sparse_model, settings.sparse_revision, True),
    ):
        repo = source_repo(name, sparse=sparse)
        found: list[str] = []
        if root is not None:
            snapshots = root / f"models--{repo.replace('/', '--')}" / "snapshots"
            if snapshots.is_dir():
                found = sorted(child.name for child in snapshots.iterdir() if child.is_dir())
        report[label] = {
            "model": name,
            "source_repo": repo,
            "expected_revision": expected,
            "cached_revisions": found,
            "matches_pin": expected in found,
        }
    return report


def verify_model_pins(settings: QdrantSettings) -> dict[str, dict[str, Any]]:
    """Raise unless the cache holds exactly the pinned commits.

    Run at image-build time. A drifted embedding model silently changes every
    vector in the collection, which looks like a retrieval-quality regression
    rather than the dependency change it is.
    """
    report = resolve_pinned_revisions(settings)
    drifted = [
        f"{label}: expected {entry['expected_revision']}, "
        f"cache has {entry['cached_revisions'] or 'nothing'}"
        for label, entry in report.items()
        if not entry["matches_pin"]
    ]
    if drifted:
        raise RuntimeError("embedding model pin mismatch — " + "; ".join(drifted))
    return report


def _to_sparse_vector(embedding: Any) -> models.SparseVector:
    return models.SparseVector(
        indices=embedding.indices.tolist(), values=embedding.values.tolist()
    )


class VectorStore:
    """Indexing and hybrid retrieval against one Qdrant collection."""

    def __init__(self, settings: QdrantSettings, embedder: Embedder | None = None) -> None:
        self._settings = settings
        self._client = QdrantClient(
            url=settings.url,
            api_key=settings.api_key,
            timeout=int(settings.timeout_seconds),
        )
        self._embedder = embedder or get_embedder(settings)

    @property
    def collection(self) -> str:
        return self._settings.collection

    # -- schema ------------------------------------------------------------

    def ensure_collection(self) -> str:
        """Create the hybrid collection if it does not exist yet."""
        if self._client.collection_exists(self.collection):
            return "exists"
        self._client.create_collection(
            collection_name=self.collection,
            vectors_config={
                DENSE_VECTOR_NAME: models.VectorParams(
                    size=self._settings.embedding_dim,
                    distance=models.Distance.COSINE,
                )
            },
            sparse_vectors_config={
                # Required for Qdrant/bm25: the client sends raw term weights and
                # the server applies IDF. Without it, BM25 scoring is wrong.
                SPARSE_VECTOR_NAME: models.SparseVectorParams(
                    modifier=models.Modifier.IDF
                )
            },
        )
        # Retrieval filters and the "documents for this locale" demo query both
        # need an index; without it Qdrant falls back to a full scan.
        self._client.create_payload_index(
            collection_name=self.collection,
            field_name="doc_id",
            field_schema=models.PayloadSchemaType.KEYWORD,
        )
        self._client.create_payload_index(
            collection_name=self.collection,
            field_name="locale",
            field_schema=models.PayloadSchemaType.KEYWORD,
        )
        return "created"

    # -- indexing ----------------------------------------------------------

    def index(self, documents: Sequence[IndexableDocument]) -> int:
        """Embed and upsert documents idempotently.

        Re-running with the same documents leaves the point count unchanged,
        which is what the replay journey asserts.
        """
        if not documents:
            return 0
        self.ensure_collection()

        texts = [f"{document.title}\n{document.text}" for document in documents]
        dense_vectors, sparse_vectors = self._embedder.embed_passages(texts)

        points = [
            models.PointStruct(
                id=stable_point_id(document.doc_id),
                vector={
                    DENSE_VECTOR_NAME: dense,
                    SPARSE_VECTOR_NAME: _to_sparse_vector(sparse),
                },
                payload={
                    "doc_id": document.doc_id,
                    "title": document.title,
                    "text": document.text,
                    "locale": document.locale,
                    "tags": list(document.tags),
                    "embedding_model_id": self._settings.embedding_model_id,
                },
            )
            for document, dense, sparse in zip(
                documents, dense_vectors, sparse_vectors, strict=True
            )
        ]
        self._client.upsert(collection_name=self.collection, points=points, wait=True)
        metrics.VECTOR_UPSERTS.labels(collection=self.collection).inc(len(points))
        metrics.VECTOR_POINTS.labels(collection=self.collection).set(self.count())
        return len(points)

    def count(self) -> int:
        return self._client.count(self.collection, exact=True).count

    # -- retrieval ---------------------------------------------------------

    def search(
        self, question: str, *, top_k: int = 3, locale: str | None = None
    ) -> list[RetrievedSource]:
        """Hybrid dense + sparse search fused with RRF.

        Note the argument is ``query_filter`` — ``Prefetch`` calls the same thing
        ``filter``, and passing ``filter=`` to ``query_points`` is silently
        swallowed by ``**kwargs`` instead of raising.
        """
        started = time.perf_counter()
        with span(
            SPAN_QDRANT_QUERY,
            attributes={
                "db.system": "qdrant",
                "db.collection.name": self.collection,
                "lab28.retrieval.mode": "hybrid",
                "lab28.retrieval.top_k": top_k,
            },
        ) as active:
            try:
                dense_query, sparse_query = self._embedder.embed_query(question)
                query_filter = (
                    models.Filter(
                        must=[
                            models.FieldCondition(
                                key="locale", match=models.MatchValue(value=locale)
                            )
                        ]
                    )
                    if locale
                    else None
                )
                response = self._client.query_points(
                    collection_name=self.collection,
                    prefetch=[
                        models.Prefetch(
                            query=dense_query,
                            using=DENSE_VECTOR_NAME,
                            limit=self._settings.prefetch_limit,
                            filter=query_filter,
                        ),
                        models.Prefetch(
                            query=_to_sparse_vector(sparse_query),
                            using=SPARSE_VECTOR_NAME,
                            limit=self._settings.prefetch_limit,
                            filter=query_filter,
                        ),
                    ],
                    query=models.FusionQuery(fusion=models.Fusion.RRF),
                    query_filter=query_filter,
                    limit=top_k,
                    with_payload=True,
                )
            except Exception as error:
                metrics.RETRIEVAL_SECONDS.labels(mode="hybrid", outcome="error").observe(
                    time.perf_counter() - started
                )
                raise RetrievalUnavailable(f"Qdrant query failed: {error}") from error

            sources = [_to_source(point) for point in response.points]
            elapsed = time.perf_counter() - started
            metrics.RETRIEVAL_SECONDS.labels(mode="hybrid", outcome="ok").observe(elapsed)
            active.set_attribute("lab28.retrieval.hits", len(sources))
            return sources

    # -- health ------------------------------------------------------------

    def health(self) -> dict[str, Any]:
        """Readiness probe: the collection must exist and hold points."""
        try:
            exists = self._client.collection_exists(self.collection)
            points = self.count() if exists else 0
            if exists:
                metrics.VECTOR_POINTS.labels(collection=self.collection).set(points)
            return {
                "reachable": True,
                "collection_exists": exists,
                "points": points,
                "detail": "ok" if exists and points else "collection empty or missing",
            }
        except Exception as error:
            return {
                "reachable": False,
                "collection_exists": False,
                "points": 0,
                "detail": f"{type(error).__name__}: {error}",
            }

    def close(self) -> None:
        self._client.close()


def _to_source(point: Any) -> RetrievedSource:
    payload = point.payload or {}
    text = str(payload.get("text", ""))
    return RetrievedSource(
        doc_id=str(payload.get("doc_id", point.id)),
        title=str(payload.get("title", "")),
        snippet=text[:SNIPPET_CHARS],
        score=float(point.score),
        retrieval_mode="hybrid",
    )


def documents_from_rows(rows: Iterable[dict[str, Any]]) -> list[IndexableDocument]:
    """Adapt Delta document rows into indexable documents."""
    return [
        IndexableDocument(
            doc_id=str(row["doc_id"]),
            title=str(row.get("title", "")),
            text=str(row.get("text", "")),
            locale=str(row.get("locale", "vi")),
            tags=tuple(row.get("tags") or ()),
        )
        for row in rows
    ]
