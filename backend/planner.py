from enum import Enum
from typing import Any, Dict, Optional
from pydantic import BaseModel, ConfigDict, Field

from open_ai_client import OpenAIHTTPClient


# - - - - - - - - - - DATA MODELS - - - - - - - - - -

class Mode(str, Enum):
    STRUCTURED = "structured"
    VECTOR = "vector"
    HYBRID = "hybrid"


class Intent(str, Enum):
    LIST = "list"
    COUNT = "count"
    EXPLAIN = "explain"
    COMPARE = "compare"
    OTHER = "other"


class QueryPlan(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    v: int = Field(default=1, ge=1)
    mode: str
    intent: str
    user_query: str
    query: str
    filters: Dict[str, Any] = Field(default_factory=dict)
    limit: int
    chunks_per_project: Optional[int] = None
    notes: Optional[str] = None


# - - - - - - - - - - HEURISTIC PLANNER (DETERMINISTIC) - - - - - - - - - -

class HeuristicPlanner:
    def __init__(self) -> None:
        self.default_limit: int = 10
        self.default_cpp: int = 3

    def plan(self, user_query: str) -> QueryPlan:
        uq = (user_query or "").strip()
        if not uq:
            return QueryPlan(
                v=1, mode="hybrid",
                intent="other",
                user_query="",
                query="",
                filters={},
                limit=1,
                notes="empty query"
            )

        return QueryPlan(
            v=1,
            mode="hybrid",
            intent="other",
            user_query=uq,
            query=uq,
            filters={},
            limit=self.default_limit,
            chunks_per_project=self.default_cpp,
            notes="fallback plan"
        )


# - - - - - - - - - - PLAN VALIDATOR FOR LLM - - - - - - - - - -

class PlanValidator:
    ALLOWED_MODE = {"structured", "vector", "hybrid"}
    ALLOWED_INTENT = {"list", "count", "explain", "compare", "other"}
    ALLOWED_FILTER_KEYS = {"project_title", "given_name", "client_name", "country", "city", "status", "year", "program", "is_ongoing"}
    ALLOWED_STATUS = {"study", "competition", "completed", "construction", "demolished", "development"}
    ALLOWED_PROGRAM = {"arena", "bar", "branding", "education", "exhibition", "gallery", "hotel", "industrial", "infrastructure",
        "landscape", "library", "masterplan", "mixed used", "museum", "office", "product design", "research", "residential",
        "restaurant", "retail", "scenography", "theatre"
    }

    def __init__(self) -> None:
        self.max_limit: int = 10
        self.max_cpp: int = 3
        self.default_limit: int = 6
        self.default_cpp: int = 2

    def validate(self, raw: Dict[str, Any]) -> QueryPlan:
        # Required base
        v = 1
        mode = str(raw.get("mode", "")).strip()
        intent = str(raw.get("intent", "")).strip()
        user_query = raw.get("user_query", "").strip()
        query = raw.get("query", "").strip()
        filters_ = raw.get("filters", {})
        limit = raw.get("limit", self.default_limit)
        
        if mode not in self.ALLOWED_MODE:
            mode = "hybrid"
        
        if intent not in self.ALLOWED_INTENT:
            intent = "other"

        if not isinstance(query, str) or len(query.strip()) < 1:
            raise ValueError("invalid query")

        if not isinstance(filters_, dict):
            filters_ = {}

        # Whitelist filters only
        filters_ = {k: v for k, v in filters_.items() if k in self.ALLOWED_FILTER_KEYS}

        # Validate project_title as a single string and list of strings
        if "project_title" in filters_:
            value = filters_["project_title"]
            if not (
                isinstance(value, str) or
                (isinstance(value, list) and all(isinstance(v, str) for v in value))
            ):
                filters_.pop("project_title", None)
        # Validate given_name as a single string and list of strings
        if "given_name" in filters_:
            value = filters_["given_name"]
            if not (
                isinstance(value, str) or
                (isinstance(value, list) and all(isinstance(v, str) for v in value))
            ):
                filters_.pop("given_name", None)
        # Validate client_name as a single string and list of strings
        if "client_name" in filters_:
            value = filters_["client_name"]
            if not (
                isinstance(value, str) or
                (isinstance(value, list) and all(isinstance(v, str) for v in value))
            ):
                filters_.pop("client_name", None)
        # Validate country as a single string and list of strings
        if "country" in filters_:
            value = filters_["country"]
            if not (
                isinstance(value, str) or
                (isinstance(value, list) and all(isinstance(v, str) for v in value))
            ):
                filters_.pop("country", None)
        # Validate city as a single string and list of strings
        if "city" in filters_:
            value = filters_["city"]
            if not (
                isinstance(value, str) or
                (isinstance(value, list) and all(isinstance(v, str) for v in value))
            ):
                filters_.pop("city", None)
        # Validate is_ongoing
        if "is_ongoing" in filters_ and not isinstance(filters_["is_ongoing"], bool):
            filters_.pop("is_ongoing", None)

        # Validate status
        if "status" in filters_ and filters_["status"] not in self.ALLOWED_STATUS:
            filters_.pop("status", None)
        # Validate program a single string and list of strings
        if "program" in filters_:
            value = filters_["program"]
            if not (
                isinstance(value, str) or
                (isinstance(value, list) and all(isinstance(v, str) for v in value))
            ):
                filters_.pop("program", None)

        # Validate year
        if "year" in filters_:
            yr = filters_["year"]
            if not (isinstance(yr, dict) and set(yr.keys()) == {"from", "to"} and isinstance(yr["from"], int) and isinstance(yr["to"], int)):
                filters_.pop("year", None)
            else:
                if yr["from"] > yr["to"]:
                    filters_["year"] = {"from": yr["to"], "to": yr["from"]}
        
        # Update given_name key
        if "given_name" in filters_:
            filters_["partners"] = filters_.pop("given_name")
        # Update client_name key
        if "client_name" in filters_:
            filters_["clients"] = filters_.pop("client_name")
        # Update project_title key
        if "project_title" in filters_:
            filters_["title"] = filters_.pop("project_title")
        # Update is_ongoing value
        if "is_ongoing" in filters_:
            filters_["is_ongoing"] = "true" if filters_["is_ongoing"] else "false"

        # Clamp limit
        try:
            limit_i = int(limit)
        except Exception:
            limit_i = self.default_limit
        limit_i = max(1, min(self.max_limit, limit_i))

        # chunks_per_project rules
        cpp = raw.get("chunks_per_project", None)
        if mode == "structured":
            cpp = None
        else:
            if cpp is None:
                cpp = self.default_cpp
            try:
                cpp = int(cpp)
            except Exception:
                cpp = self.default_cpp
            cpp = max(1, min(self.max_cpp, cpp))

        notes = raw.get("notes")
        if notes is not None and not isinstance(notes, str):
            notes = None

        return QueryPlan(
            v=v,
            mode=mode,
            intent=intent,
            user_query=user_query,
            query=query,
            filters=filters_,
            limit=limit_i,
            chunks_per_project=cpp,
            notes=notes,
        )


# - - - - - - - - - - PLANNER ORCHESTRATOR - - - - - - - - - -

class Planner:
    def __init__(self) -> None:
        self.llm: OpenAIHTTPClient = OpenAIHTTPClient(model="gpt-4o-mini")
        self.heuristic: HeuristicPlanner = HeuristicPlanner()
        self.validator: PlanValidator = PlanValidator()
        self.last_usage: dict = {}


    # - - - - - Public API - - - - -
    def plan(self, user_query: str) -> QueryPlan:
        uq = (user_query or "").strip()
        if not uq:
            self.last_usage = {}
            return self.heuristic.plan(uq)
        return self._llm_path(uq)


    # - - - - - Helpers - - - - -
    def _llm_path(self, user_query: str) -> Optional[QueryPlan]:
        schema_hint = self._schema_hint()
        try:
            drafted = self.llm.draft_plan_json(user_query=user_query, schema_hint=schema_hint)
            self.last_usage = self.llm.last_usage
            drafted['user_query'] = user_query
            if not drafted.get('query', '').strip():
                drafted['query'] = user_query
        except Exception as e:
            self.last_usage = {}
            hp = self.heuristic.plan(user_query)
            hp.notes = f"fallback=llm_error({type(e).__name__}) | path=heuristic"
            return hp

        # Validator + fallback if drafted plan unusable
        try:
            valid = self.validator.validate(drafted)
            valid.notes = (valid.notes or "llm") + " | path=llm"
            return valid
        except Exception as e:
            hp = self.heuristic.plan(user_query)
            hp.notes = f"fallback=validator_reject({type(e).__name__}) | path=heuristic"
            return hp

    def _schema_hint(self) -> str:
        return ("""Return ONE JSON object that matches the schema hint exactly. No markdown. No comments. No extra keys.
Allowed keys: mode, intent, filters, query.

Core planning rules:
1) mode selection:
   - structured: lists/counts based mainly on metadata fields
   - vector: needs descriptive text evidence (design concept, narrative, explanation, qualities)
   - hybrid: needs both metadata + text evidence
   If filters is non-empty and intent is explain/compare, mode must be hybrid.
   Creative requests (poem, story, slogan) still require evidence; mode must be hybrid.

2) intent selection:
   list|count|explain|compare|other
   Creative outputs usually use intent="other" unless the user also asked to explain/compare.

3) filters ONLY when explicitly stated by the user.
   Allowed filters: project_title, given_name, client_name, country, city,
   status(study|competition|completed|construction|demolished|development),
   year{from,to}, program(arena|bar|branding|education|exhibition|gallery|hotel|industrial|infrastructure|landscape|
   library|masterplan|mixed use|museum|office|product design|research|residential|restaurant|retail|scenography|theatre),
   is_ongoing(true|false)

4) Identify whether the user mentions a specific OMA PROJECT TITLE.
   Treat the following as strong signals that a project title is present:
   - Any text in single or double quotes after words like: project, building, scheme, proposal.
     Example: "project 'Casa Palestra'" -> project_title = "Casa Palestra"
   - Patterns like: project named X, project called X, based on the project X
   - Capitalized multi-word proper nouns can be a candidate ONLY if the user context says it is a project.
   Do NOT invent a project_title. If unsure, leave it empty.

5) query optimization:
   The "query" field must be optimized for retrieval, not for following instructions.
   - The lenght of the query must be equal than or shorter than user_query
   - If project_title is present, set query to something like:
     "description and design concept of {project_title}"
     (Do NOT include "write a poem", "make a poem", or other generation instructions.)
   - If no project_title, keep query close to the informational need.
"""
        )