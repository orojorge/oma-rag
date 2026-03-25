# Ingestion

Ingestion pipeline for OMA-RAG. Transforms raw HTML project pages into indexed, searchable chunks in OpenSearch.

The pipeline is intentionally sequential with no orchestrator. Each script produces an artifact that can be inspected and corrected before the next step runs. Where human review is useful, the next script will automatically use the reviewed version if it exists. The system will automatically prefer the reviewed version over the original.

---

## Pipeline

Each step writes a standalone artifact to `ingestion/artifacts` before the next step runs. This makes it possible to inspect and correct the output at any stage — in particular after `parser.py` (raw text quality) and `extract_specs.py` (structured metadata) — without re-running earlier steps. The embedding and indexing steps are fully automated and require no review.

```
Raw HTML → parser.py → extract_specs.py → chunker.py → embedder.py → streamer.py → OpenSearch
```

---

## Artifact Map


| Script             | Input                                                      | Output                                       | Human Review                                                              |
| ------------------ | ---------------------------------------------------------- | -------------------------------------------- | ------------------------------------------------------------------------- |
| `parser.py`        | `data/oma/projects/` (raw HTML)                            | `projects.jsonl`                             | Check for typos in project descriptions                                   |
| `extract_specs.py` | `projects.jsonl`                                           | `specs.json`                                 | Check for typos in structured metadata (location, typology, client, year) |
| `chunker.py`       | `projects.jsonl` + `specs_reviewed.json` or `specs.json`   | `bulk_projects.ndjson`, `bulk_chunks.ndjson` | None                                                                      |
| `embedder.py`      | `bulk_chunks.ndjson`                                       | `bulk_chunks_w_embeddings.ndjson`            | None                                                                      |
| `streamer.py`      | `bulk_projects.ndjson` + `bulk_chunks_w_embeddings.ndjson` | loads into OpenSearch                        | None                                                                      |


All artifacts are written to `ingestion/artifacts`.

---

## Human Review

After `parser.py` and `extract_specs.py`, review the output before continuing. To apply corrections, edit the artifact directly and save as the reviewed filename:

- `artifacts/specs.json` → save corrections as `artifacts/specs_reviewed.json`

`chunker.py` will automatically use `specs_reviewed.json` if it exists, otherwise fall back to `specs.json`.

---

## Run

```bash
pip install -r ingestion/requirements.txt
curl -L https://paraclet.s3.eu-central-1.amazonaws.com/oma.zip -o oma.zip
unzip oma.zip -d ingestion/data
make ingest
# or manually:
cd ingestion
python parser.py
python extract_specs.py
# review artifacts/specs.json, save corrections as artifacts/specs_reviewed.json
python chunker.py
python embedder.py
python streamer.py
```

---

## Aliases

`aliases.py` is a separate utility, not part of the ingestion sequence. It reads the specs artifact and generates `backend/aliases.json`, a vocabulary and alias map consumed by the backend Normalizer to filter search terms at query time.

```bash
python aliases.py
# review backend/aliases.json
# save corrections as backend/aliases_reviewed.json
```

Fields indexed: `status`, `program`, `city`, `country`.

The backend will automatically use `aliases_reviewed.json` if it exists, otherwise fall back to `aliases.json`.