# Backend

FastAPI backend for OMA-RAG. Receives natural language queries and returns grounded, cited answers by orchestrating a pipeline of planning, retrieval, fusion, and synthesis stages.

---

## Pipeline

The Planner classifies each query into one of three retrieval modes: **structured** (metadata filters only, e.g. "offices built in Rotterdam after 2000"), **vector** (embedding search over descriptive text, e.g. "projects exploring ideas of porosity"), or **hybrid** (both). The rest of the pipeline adapts accordingly — structured queries skip the vector index entirely; vector queries backfill project metadata from the structured index; hybrid queries run both paths and merge the results.

```
Query → Planner → Normalizer → Retrieval → Fusion → Synthesizer → Validator → Answer
```

---

## Module Map

| Module                    | Role                                                                                                                                                                    |
| ------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `planner.py`              | Interprets the query via `gpt-4o-mini` and produces a structured `QueryPlan` (mode, intent, filters, limit). Falls back to a deterministic heuristic plan on LLM error. |
| `normalizer.py`           | Expands filter values using a domain vocabulary and alias map (`aliases.json`), correcting spelling variants and synonyms before retrieval.                             |
| `structured_retriever.py` | Queries OpenSearch metadata fields (title, country, city, status, program, year, partners, clients) using term and range filters.                                       |
| `vector_retriever.py`     | Runs kNN embedding search against the OpenSearch chunks index using `text-embedding-3-small`.                                                                           |
| `fusion.py`               | Merges structured and vector results, caps chunks per project, sorts by relevance score, and applies the plan limit.                                                    |
| `synthesis.py`            | Builds the context prompt from fused evidence, streams a cited answer via `gpt-4.1-mini`, and validates citations post-generation.                                      |
| `open_ai_client.py`       | Thin HTTP client wrapping OpenAI Chat Completions and Responses APIs (`draft_plan_json`, `chat`, `chat_stream`, `embed_text`).                                          |
| `monitoring.py`           | Emits per-request structured logs (latencies, token counts, plan metadata).                                                                                             |
| `api.py`                  | FastAPI app. Exposes `POST /query` (blocking) and `POST /query/stream` (SSE).                                                                                           |

---

## Endpoints

| Method | Path            | Description                                                                                       |
| ------ | --------------- | ------------------------------------------------------------------------------------------------- |
| `GET`  | `/health`       | Health check                                                                                      |
| `POST` | `/query`        | Returns a complete `QueryResponse` (answer, citations, warnings)                                  |
| `POST` | `/query/stream` | Streams tokens as Server-Sent Events (SSE), then a final `done` event with citations and warnings |

---

## Development

`cli.py` is a REPL for local development — not part of the request pipeline. Use it to run queries interactively, inspect pipeline outputs, and monitor per-stage latencies without going through the API.

```bash
python cli.py
```

---

## Environment Variables

```bash
OPENAI_BASE_URL=https://api.openai.com/v1
OPENAI_API_KEY=...
OPENSEARCH_HOST=http://localhost:9200
```

---

## Aliases

The Normalizer loads `aliases_reviewed.json` if present, otherwise `aliases.json`. Generate and review this file using `ingestion/aliases.py`.

---

## Run

```bash
pip install -r requirements.txt
uvicorn api:app --host 0.0.0.0 --port 8000
```