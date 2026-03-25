from typing import Any, Dict, List, Optional, Tuple
from pydantic import BaseModel, ConfigDict, Field

from planner import QueryPlan
from open_ai_client import OpenAIHTTPClient
from retrieval_utils import as_str, as_str_list, as_int, as_bool, extract_total


# - - - - - - - - - - DATA MODELS - - - - - - - - - -

class ChunkCard(BaseModel):
    model_config = ConfigDict(extra="forbid")

    project_id: str
    title: str

    chunk_id: str
    chunk_index: Optional[int] = None
    chunk_text: str
    score: Optional[float] = None

    citation_id: str


class VectorRetrievalResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    chunks: List[ChunkCard] = Field(default_factory=list)
    total: Optional[int] = None
    applied_filters: Dict[str, Any] = Field(default_factory=dict)
    warnings: List[str] = Field(default_factory=list)
    candidate_project_ids: List[str] = Field(default_factory=list)


# - - - - - - - - - - VECTOR RETRIEVER (ORCHESTRATOR) - - - - - - - - - -

class VectorRetriever:
    def __init__(
        self,
        *,
        os_client: Any,
        max_limit: int = 50,
        max_k: int = 1000,
        k_factor: int = 3,
        num_candidates_factor: int = 5,
    ) -> None:
        self._os = os_client
        self._index = "chunks_current"
        self._embedder: OpenAIHTTPClient = OpenAIHTTPClient(model="text-embedding-3-small")
        self._max_limit = max_limit
        self._max_k = max_k
        self._k_factor = max(1, int(k_factor))
        self._num_candidates_factor = max(1, int(num_candidates_factor))
        self._builder = ChunkQueryBuilder()
        self._mapper = ChunkResultMapper()

    # - - - - - Public API - - - - -
    def retrieve(self, plan: QueryPlan) -> VectorRetrievalResult:
        if plan.mode not in ("vector", "hybrid"):
            return VectorRetrievalResult(
                chunks=[],
                total=0,
                applied_filters={},
                warnings=[f"VectorRetriever skipped because plan.mode={plan.mode!r}"],
                candidate_project_ids=[],
            )

        size = max(0, min(int(plan.limit), self._max_limit))
        if size == 0:
            return VectorRetrievalResult(
                chunks=[],
                total=0,
                applied_filters={},
                warnings=["VectorRetriever skipped because size=0"],
                candidate_project_ids=[],
            )

        query = (plan.query or "").strip()
        if not query:
            return VectorRetrievalResult(
                chunks=[],
                total=0,
                applied_filters={},
                warnings=["VectorRetriever skipped because query was empty"],
                candidate_project_ids=[],
            )

        embedding = self._embedder.embed_text(text=query)
        if not isinstance(embedding, list) or len(embedding) == 0:
            return VectorRetrievalResult(
                chunks=[],
                total=0,
                applied_filters={},
                warnings=["VectorRetriever failed because embedding was empty"],
                candidate_project_ids=[],
            )

        cpp = max(1, int(plan.chunks_per_project or 1))
        k = min(self._max_k, max(1, size * cpp * self._k_factor))
        num_candidates = min(self._max_k, max(k, k * self._num_candidates_factor))
        body, applied, warnings = self._builder.build_search(
            plan,
            embedding=embedding,
            size=size,
            k=k,
            num_candidates=num_candidates,
            chunks_per_project=cpp,
        )
        resp = self._os.search(index=self._index, body=body)
        hits = (((resp or {}).get("hits") or {}).get("hits") or [])
        chunks = self._mapper.to_chunks(hits, expected_inner_hits=cpp)

        # Enforce chunks_per_project defensively even if collapse failed.
        chunks = _cap_chunks_per_project(chunks, cpp)

        return VectorRetrievalResult(
            chunks=chunks,
            total=extract_total(resp),
            applied_filters=applied,
            warnings=warnings,
            candidate_project_ids=_unique_project_ids(chunks),
        )


# - - - - - - - - - - BUILDER - - - - - - - - - -

class ChunkQueryBuilder:
    DEFAULT_SOURCE_INCLUDES: Tuple[str, ...] = (
        "chunk_id",
        "project_id",
        "title",
        "chunk_text",
        "chunk_index",
        "status",
        "program",
        "partners",
        "clients",
        "city",
        "country",
        "year_start",
        "year_end",
        "is_ongoing",
    )

    def build_search(
        self,
        plan: QueryPlan,
        *,
        embedding: List[float],
        size: int,
        k: int,
        num_candidates: int,
        chunks_per_project: int,
    ) -> Tuple[Dict[str, Any], Dict[str, Any], List[str]]:
        filters_dsl, applied_filters, warnings = self._build_filters(plan.filters)

        body: Dict[str, Any] = {
            "size": size,
            "_source": {"includes": list(self.DEFAULT_SOURCE_INCLUDES)},
            "query": {
                "bool": {
                    "filter": filters_dsl,
                    "must": [
                        {
                            "knn": {
                                "embedding": {
                                    "vector": embedding,
                                    "k": k,
                                }
                            }
                        }
                    ],
                }
            },
            "collapse": {
                "field": "project_id.keyword",
                "inner_hits": {
                    "name": "top_chunks",
                    "size": chunks_per_project,
                    "_source": {"includes": list(self.DEFAULT_SOURCE_INCLUDES)},
                },
            },
        }

        return body, applied_filters, warnings

    def _build_filters(self, filters: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any], List[str]]:
        out: List[Dict[str, Any]] = []
        applied: Dict[str, Any] = {}
        warnings: List[str] = []

        for key, raw_val in (filters or {}).items():
            if raw_val is None:
                continue

            if key == "title":
                val = as_str(raw_val)
                if val:
                    out.append({"match": {"title": val}})
                    applied[key] = val
                continue

            if key == "status":
                val = as_str(raw_val)
                if val:
                    out.append({"term": {"status.keyword": val}})
                    applied[key] = val
                continue

            if key == "program":
                vals = as_str_list(raw_val)
                if vals:
                    out.append({"terms": {"program.keyword": vals}})
                    applied[key] = vals
                continue

            if key == "partners":
                vals = as_str_list(raw_val)
                if vals:
                    out.append({
                        "bool": {
                            "should": [{"match": {"partners": v}} for v in vals],
                            "minimum_should_match": 1,
                        }
                    })
                    applied[key] = vals
                continue

            if key == "clients":
                vals = as_str_list(raw_val)
                if vals:
                    out.append({
                        "bool": {
                            "should": [{"match": {"clients": v}} for v in vals],
                            "minimum_should_match": 1,
                        }
                    })
                    applied[key] = vals
                continue

            if key == "city":
                vals = as_str_list(raw_val)
                if vals:
                    out.append({"terms": {"city.keyword": vals}})
                    applied[key] = vals
                continue

            if key == "country":
                vals = as_str_list(raw_val)
                if vals:
                    out.append({"terms": {"country.keyword": vals}})
                    applied[key] = vals
                continue

            if key == "is_ongoing":
                b = as_bool(raw_val)
                if b is not None:
                    out.append({"term": {"is_ongoing": b}})
                    applied[key] = b
                continue

            if key == "year":
                if isinstance(raw_val, dict):
                    y_from = as_int(raw_val.get("from"))
                    y_to = as_int(raw_val.get("to"))

                    if y_from is None and y_to is None:
                        warnings.append("Ignored filter 'year' because both 'from' and 'to' were missing/invalid")
                        continue

                    if y_to is not None:
                        out.append({"range": {"year_start": {"lte": y_to}}})
                    if y_from is not None:
                        out.append({"range": {"year_end": {"gte": y_from}}})

                    applied[key] = {"from": y_from, "to": y_to}
                    continue

            warnings.append(f"Ignored unknown filter key: {key!r}")

        return out, applied, warnings


# - - - - - - - - - - MAPPER - - - - - - - - - -

class ChunkResultMapper:
    def to_chunks(self, hits: List[Dict[str, Any]], expected_inner_hits: int) -> List[ChunkCard]:
        chunks: List[ChunkCard] = []

        for h in hits:
            inner = (((h.get("inner_hits") or {}).get("top_chunks") or {}).get("hits") or {}).get("hits") or []
            if inner:
                for ih in inner:
                    c = self._hit_to_chunk(ih)
                    if c:
                        chunks.append(c)
                continue

            # Fallback if collapse didn't return inner hits
            c = self._hit_to_chunk(h)
            if c:
                chunks.append(c)

        return chunks

    def _hit_to_chunk(self, h: Dict[str, Any]) -> Optional[ChunkCard]:
        src = h.get("_source") or {}
        project_id = as_str(src.get("project_id"))
        title = as_str(src.get("title"))
        chunk_text = as_str(src.get("chunk_text"))
        if not project_id or not title or not chunk_text:
            return None

        chunk_id = as_str(src.get("chunk_id")) or f"{project_id}:{as_int(src.get('chunk_index')) or 0}"
        chunk_index = as_int(src.get("chunk_index"))
        score = h.get("_score") if isinstance(h.get("_score"), (int, float)) else None

        return ChunkCard(
            project_id=project_id,
            title=title,
            chunk_id=chunk_id,
            chunk_index=chunk_index,
            chunk_text=chunk_text,
            score=score,
            citation_id=f"C:{chunk_id}",
        )


# - - - - - - - - - - UTILS - - - - - - - - - -

def _cap_chunks_per_project(chunks: List[ChunkCard], limit: int) -> List[ChunkCard]:
    if limit <= 0:
        return []
    seen: Dict[str, int] = {}
    out: List[ChunkCard] = []
    for c in chunks:
        n = seen.get(c.project_id, 0)
        if n >= limit:
            continue
        out.append(c)
        seen[c.project_id] = n + 1
    return out


def _unique_project_ids(chunks: List[ChunkCard]) -> List[str]:
    seen: Dict[str, None] = {}
    for c in chunks:
        seen[c.project_id] = None
    return list(seen.keys())
