from typing import Any, Dict, List, Optional, Tuple, Union
from pydantic import BaseModel, ConfigDict, Field

from planner import QueryPlan
from retrieval_utils import as_str, as_str_list, as_int, as_bool, extract_total


# - - - - - - - - - - DATA MODELS - - - - - - - - - -

class ProjectCard(BaseModel):
    model_config = ConfigDict(extra="forbid")

    project_id: str
    title: str

    status: Optional[str] = None
    program: List[str] = Field(default_factory=list)
    partners: List[str] = Field(default_factory=list)
    clients: List[str] = Field(default_factory=list)

    city: List[str] = Field(default_factory=list)
    country: List[str] = Field(default_factory=list)

    year_start: Optional[int] = None
    year_end: Optional[int] = None
    is_ongoing: Optional[bool] = None

    location_raw: Optional[str] = None
    year_raw: Optional[str] = None

    citation_id: str


class StructuredRetrievalResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    projects: List[ProjectCard] = Field(default_factory=list)
    total: Optional[int] = None
    applied_filters: Dict[str, Any] = Field(default_factory=dict)
    warnings: List[str] = Field(default_factory=list)
    candidate_project_ids: List[str] = Field(default_factory=list)


# - - - - - - - - - - STRUCTURED RETRIEVER (ORCHESTRATOR) - - - - - - - - - -

class StructuredRetriever:
    def __init__(self, *, os_client: Any, max_limit: int = 25) -> None:
        self._os = os_client
        self._index = "projects_current"
        self._max_limit = max_limit
        self._builder = ProjectQueryBuilder()
        self._mapper = ProjectResultMapper()

    # - - - - - Public API - - - - -
    def retrieve(self, plan: QueryPlan) -> StructuredRetrievalResult:
        if plan.mode not in ("structured", "hybrid"):
            return StructuredRetrievalResult(
                projects=[],
                total=0,
                applied_filters={},
                warnings=[f"StructuredRetriever skipped because plan.mode={plan.mode!r}"],
                candidate_project_ids=[],
            )

        size = max(0, min(int(plan.limit), self._max_limit))

        if plan.intent == "count":
            body, applied, warnings = self._builder.build_count(plan)
            resp = self._os.search(index=self._index, body=body)
            return StructuredRetrievalResult(
                projects=[],
                total=extract_total(resp),
                applied_filters=applied,
                warnings=warnings,
                candidate_project_ids=[],
            )

        # list / explain / compare / other -> return project cards
        sort = self._choose_sort(plan)
        body, applied, warnings = self._builder.build_search(
            plan,
            size=size,
            track_total_hits=False,
            sort=sort,
        )
        resp = self._os.search(index=self._index, body=body)
        hits = (((resp or {}).get("hits") or {}).get("hits") or [])
        cards = self._mapper.to_cards(hits)

        return StructuredRetrievalResult(
            projects=cards,
            total=extract_total(resp),
            applied_filters=applied,
            warnings=warnings,
            candidate_project_ids=[c.project_id for c in cards],
        )

    def fetch(self, project_ids: list[str]) -> StructuredRetrievalResult:
        if not project_ids:
            return StructuredRetrievalResult()

        body = {
            "size": len(project_ids),
            "_source": {"includes": list(ProjectQueryBuilder.DEFAULT_SOURCE_INCLUDES)},
            "query": {"ids": {"values": project_ids}},
        }
        resp = self._os.search(index=self._index, body=body)
        hits = (((resp or {}).get("hits") or {}).get("hits") or [])
        cards = self._mapper.to_cards(hits)
        return StructuredRetrievalResult(
            projects=cards,
            total=len(cards),
            candidate_project_ids=[c.project_id for c in cards],
        )

    # - - - - - Private - - - - -
    def _choose_sort(self, plan: QueryPlan) -> List[Dict[str, Any]]:
        q = (plan.query or "").lower()
        notes = (plan.notes or "").lower()

        if any(k in q for k in ("latest", "most recent", "newest")) or any(
            k in notes for k in ("latest", "most recent", "newest")
        ):
            return [
                {"year_end": {"order": "desc", "missing": "_last"}},
                {"title.keyword": {"order": "asc", "missing": "_last"}},
            ]

        if any(k in q for k in ("earliest", "oldest", "first")) or any(
            k in notes for k in ("earliest", "oldest", "first")
        ):
            return [
                {"year_start": {"order": "asc", "missing": "_last"}},
                {"title.keyword": {"order": "asc", "missing": "_last"}},
            ]

        # Default: stable alphabetical.
        return [{"title.keyword": {"order": "asc", "missing": "_last"}}]


# - - - - - - - - - - BUILDER - - - - - - - - - -

class ProjectQueryBuilder:
    DEFAULT_SOURCE_INCLUDES: Tuple[str, ...] = (
        "title",
        "status",
        "program",
        "partners",
        "clients",
        "city",
        "country",
        "year_start",
        "year_end",
        "is_ongoing",
        "location_raw",
        "year_raw",
    )

    # - - - - - API - - - - -
    def build_search(
        self,
        plan: QueryPlan,
        *,
        size: int,
        track_total_hits: Union[bool, int] = False,
        sort: Optional[List[Dict[str, Any]]] = None,
    ) -> Tuple[Dict[str, Any], Dict[str, Any], List[str]]:
        filters_dsl, applied_filters, warnings = self._build_filters(plan.filters)

        body: Dict[str, Any] = {
            "size": size,
            "_source": {"includes": list(self.DEFAULT_SOURCE_INCLUDES)},
            "query": {
                "bool": {
                    "filter": filters_dsl,
                }
            },
        }

        if track_total_hits is not False:
            body["track_total_hits"] = track_total_hits

        if sort:
            body["sort"] = sort

        return body, applied_filters, warnings

    def build_count(self, plan: QueryPlan) -> Tuple[Dict[str, Any], Dict[str, Any], List[str]]:
        filters_dsl, applied_filters, warnings = self._build_filters(plan.filters)
        body: Dict[str, Any] = {
            "size": 0,
            "track_total_hits": True,
            "query": {"bool": {"filter": filters_dsl}},
        }
        return body, applied_filters, warnings


    # - - - - - Utils - - - - -
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
                            "minimum_should_match": 1
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

class ProjectResultMapper:
    def to_cards(self, hits: List[Dict[str, Any]]) -> List[ProjectCard]:
        cards: List[ProjectCard] = []
        for h in hits:
            src = h.get("_source") or {}
            project_id = (src.get("project_id") or h.get("_id") or "").strip()
            title = (src.get("title") or "").strip()

            # Defensive: skip malformed docs
            if not project_id or not title:
                continue

            cards.append(
                ProjectCard(
                    project_id=project_id,
                    title=title,
                    status=as_str(src.get("status")),
                    program=as_str_list(src.get("program")),
                    partners=as_str_list(src.get("partners")),
                    clients=as_str_list(src.get("clients")),
                    city=as_str_list(src.get("city")),
                    country=as_str_list(src.get("country")),
                    year_start=as_int(src.get("year_start")),
                    year_end=as_int(src.get("year_end")),
                    is_ongoing=as_bool(src.get("is_ongoing")),
                    location_raw=as_str(src.get("location_raw")),
                    year_raw=as_str(src.get("year_raw")),
                    citation_id=f"P:{project_id}",
                )
            )
        return cards
