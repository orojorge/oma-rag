import json
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse

import boto3
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from opensearchpy import OpenSearch, RequestsAWSV4SignerAuth, RequestsHttpConnection
from pydantic import BaseModel, Field

load_dotenv()

from planner import Planner, QueryPlan
from normalizer import Normalizer
from structured_retriever import StructuredRetriever, StructuredRetrievalResult
from vector_retriever import VectorRetriever, VectorRetrievalResult
from fusion import FusionLayer
from synthesis import ContextBuilder, Synthesizer, Validator
from monitoring import emit_query_log


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# - - - - - - - - - - REQUEST / RESPONSE MODELS - - - - - - - - - -

class QueryRequest(BaseModel):
    query: str = Field(min_length=1, max_length=500)


class QueryResponse(BaseModel):
    answer: str
    citations: list[str]
    warnings: list[str]


# - - - - - - - - - - APP SETUP - - - - - - - - - -

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.planner = Planner()
    aliases_path = "aliases_reviewed.json" if os.path.exists("aliases_reviewed.json") else "aliases.json"
    app.state.normalizer = Normalizer(aliases_path)
    os_host = os.environ["OPENSEARCH_HOST"]
    parsed = urlparse(os_host)
    is_aws = parsed.scheme == "https"
    os_kwargs: dict = {
        "hosts": [{"host": parsed.hostname, "port": parsed.port or (443 if is_aws else 9200)}],
        "http_compress": True,
        "use_ssl": is_aws,
        "verify_certs": is_aws,
    }
    if is_aws:
        credentials = boto3.Session().get_credentials()
        region = os.environ["AWS_REGION"]
        os_kwargs["http_auth"] = RequestsAWSV4SignerAuth(credentials, region, "es")
        os_kwargs["connection_class"] = RequestsHttpConnection
    app.state.os_client = OpenSearch(**os_kwargs)
    app.state.structured_retriever = StructuredRetriever(os_client=app.state.os_client)
    app.state.vector_retriever = VectorRetriever(os_client=app.state.os_client)
    app.state.fusion_layer = FusionLayer()
    app.state.context_builder = ContextBuilder()
    app.state.synthesizer = Synthesizer()
    app.state.validator = Validator()
    yield
    app.state.os_client.close()


app = FastAPI(title="OMA-RAG", lifespan=lifespan)


# - - - - - - - - - - ENDPOINTS - - - - - - - - - -

@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/query", response_model=QueryResponse)
def query(body: QueryRequest, request: Request) -> QueryResponse:
    trace_id = uuid.uuid4().hex
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    t_start = time.perf_counter()
    try:
        t0 = time.perf_counter()
        plan = request.app.state.planner.plan(body.query)
        plan = request.app.state.normalizer.normalize(plan)
        t_plan = int((time.perf_counter() - t0) * 1000)
        planner_usage = request.app.state.planner.last_usage

        t0 = time.perf_counter()
        structured_result, vector_result = _retrieve(
            plan,
            request.app.state.structured_retriever,
            request.app.state.vector_retriever,
        )
        t_retrieval = int((time.perf_counter() - t0) * 1000)

        t0 = time.perf_counter()
        fused = request.app.state.fusion_layer.fuse(plan, structured_result, vector_result)
        ctx = request.app.state.context_builder.build(plan=plan, fused=fused)
        t_fusion = int((time.perf_counter() - t0) * 1000)

        t0 = time.perf_counter()
        answer = request.app.state.synthesizer.synthesize(ctx)
        t_synthesis = int((time.perf_counter() - t0) * 1000)
        synthesis_usage = request.app.state.synthesizer.last_usage

        t0 = time.perf_counter()
        validated = request.app.state.validator.validate(answer=answer, fused=fused)
        t_validation = int((time.perf_counter() - t0) * 1000)

        emit_query_log({
            "trace_id": trace_id,
            "ts": ts,
            "query": body.query[:200],
            "status": "ok",
            "plan_mode": plan.mode,
            "plan_intent": plan.intent,
            "filter_count": len(plan.filters),
            "plan_path": "heuristic" if plan.notes and "path=heuristic" in plan.notes else "llm",
            "projects_retrieved": len(fused.projects),
            "chunks_retrieved": sum(len(fp.chunks) for fp in fused.projects),
            "citations": len(validated.citations),
            "warnings_count": len(validated.warnings),
            "planner_ms": t_plan,
            "retrieval_ms": t_retrieval,
            "fusion_ms": t_fusion,
            "synthesis_ms": t_synthesis,
            "validation_ms": t_validation,
            "total_ms": int((time.perf_counter() - t_start) * 1000),
            "planner_tokens_in": planner_usage.get("prompt_tokens", 0),
            "planner_tokens_out": planner_usage.get("completion_tokens", 0),
            "synthesis_tokens_in": synthesis_usage.get("prompt_tokens", 0),
            "synthesis_tokens_out": synthesis_usage.get("completion_tokens", 0),
        })
        return QueryResponse(
            answer=validated.answer,
            citations=validated.citations,
            warnings=validated.warnings,
        )
    except Exception:
        logger.exception("request failed")
        emit_query_log({
            "trace_id": trace_id,
            "ts": ts,
            "query": body.query[:200],
            "status": "error",
            "total_ms": int((time.perf_counter() - t_start) * 1000),
        })
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error"},
        )


@app.post("/query/stream")
def query_stream(body: QueryRequest, request: Request):
    trace_id = uuid.uuid4().hex
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    t_start = time.perf_counter()
    try:
        t0 = time.perf_counter()
        plan = request.app.state.planner.plan(body.query)
        plan = request.app.state.normalizer.normalize(plan)
        t_plan = int((time.perf_counter() - t0) * 1000)
        planner_usage = request.app.state.planner.last_usage

        t0 = time.perf_counter()
        structured_result, vector_result = _retrieve(
            plan,
            request.app.state.structured_retriever,
            request.app.state.vector_retriever,
        )
        t_retrieval = int((time.perf_counter() - t0) * 1000)

        t0 = time.perf_counter()
        fused = request.app.state.fusion_layer.fuse(plan, structured_result, vector_result)
        ctx = request.app.state.context_builder.build(plan=plan, fused=fused)
        t_fusion = int((time.perf_counter() - t0) * 1000)

        def event_stream():
            t0 = time.perf_counter()
            full_answer = ""
            for token in request.app.state.synthesizer.synthesize_stream(ctx):
                full_answer += token
                yield f"data: {json.dumps({'type': 'token', 'token': token})}\n\n"
            t_synthesis = int((time.perf_counter() - t0) * 1000)
            synthesis_usage = request.app.state.synthesizer.last_usage

            t0 = time.perf_counter()
            validated = request.app.state.validator.validate(answer=full_answer, fused=fused)
            t_validation = int((time.perf_counter() - t0) * 1000)

            emit_query_log({
                "trace_id": trace_id,
                "ts": ts,
                "query": body.query[:200],
                "status": "ok",
                "plan_mode": plan.mode,
                "plan_intent": plan.intent,
                "filter_count": len(plan.filters),
                "plan_path": "heuristic" if plan.notes and "path=heuristic" in plan.notes else "llm",
                "projects_retrieved": len(fused.projects),
                "chunks_retrieved": sum(len(fp.chunks) for fp in fused.projects),
                "citations": len(validated.citations),
                "warnings_count": len(validated.warnings),
                "planner_ms": t_plan,
                "retrieval_ms": t_retrieval,
                "fusion_ms": t_fusion,
                "synthesis_ms": t_synthesis,
                "validation_ms": t_validation,
                "total_ms": int((time.perf_counter() - t_start) * 1000),
                "planner_tokens_in": planner_usage.get("prompt_tokens", 0),
                "planner_tokens_out": planner_usage.get("completion_tokens", 0),
                "synthesis_tokens_in": synthesis_usage.get("prompt_tokens", 0),
                "synthesis_tokens_out": synthesis_usage.get("completion_tokens", 0),
            })
            yield f"data: {json.dumps({'type': 'done', 'citations': validated.citations, 'warnings': validated.warnings})}\n\n"

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
                "Connection": "keep-alive",
            },
        )

    except Exception:
        logger.exception("stream request failed")
        emit_query_log({
            "trace_id": trace_id,
            "ts": ts,
            "query": body.query[:200],
            "status": "error",
            "total_ms": int((time.perf_counter() - t_start) * 1000),
        })
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error"},
        )


# - - - - - - - - - - HELPERS - - - - - - - - - -

def _retrieve(
    plan: QueryPlan,
    structured_retriever: StructuredRetriever,
    vector_retriever: VectorRetriever,
) -> tuple[Optional[StructuredRetrievalResult], Optional[VectorRetrievalResult]]:
    structured_result = None
    vector_result = None

    if plan.mode == "structured":
        structured_result = structured_retriever.retrieve(plan)

    elif plan.mode == "vector":
        vector_result = vector_retriever.retrieve(plan)
        structured_result = structured_retriever.fetch(vector_result.candidate_project_ids)

    elif plan.mode == "hybrid":
        has_filters = bool(plan.filters)

        if not has_filters:
            vector_result = vector_retriever.retrieve(plan)
            structured_result = structured_retriever.fetch(vector_result.candidate_project_ids)
        else:
            structured_result = structured_retriever.retrieve(plan)
            vector_result = vector_retriever.retrieve(plan)
            new_ids = [
                pid for pid in vector_result.candidate_project_ids
                if pid not in set(structured_result.candidate_project_ids)
            ]
            if new_ids:
                backfill = structured_retriever.fetch(new_ids)
                structured_result.projects.extend(backfill.projects)
                structured_result.candidate_project_ids.extend(backfill.candidate_project_ids)

    return structured_result, vector_result