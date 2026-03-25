import json
from datetime import datetime, timezone
from pathlib import Path

ARTIFACTS_DIR = Path("artifacts")
REVIEWED_SPECS = ARTIFACTS_DIR / "specs_reviewed.json"
DEFAULT_SPECS = ARTIFACTS_DIR / "specs.json"

IN_SPECS = REVIEWED_SPECS if REVIEWED_SPECS.exists() else DEFAULT_SPECS
IN_PROJECTS = ARTIFACTS_DIR / "projects.jsonl"
OUT_PROJECTS_BULK = ARTIFACTS_DIR / "bulk_projects.ndjson"
OUT_CHUNKS_BULK = ARTIFACTS_DIR / "bulk_chunks.ndjson"


def chunk_text(text: str, *, max_chars: int = 2000, overlap_chars: int = 300) -> list[str]:
    text = (text or "").strip()
    if not text:
        return []
    paras = [p.strip() for p in text.split("\n") if p.strip()]
    joined = "\n\n".join(paras)

    chunks = []
    i = 0
    while i < len(joined):
        j = min(len(joined), i + max_chars)
        chunk = joined[i:j].strip()
        if chunk:
            chunks.append(chunk)
        i = max(0, j - overlap_chars)
        if j == len(joined):
            break
    return chunks


def load_specs() -> dict:
    with IN_SPECS.open("r", encoding="utf-8") as f:
        all_specs = json.load(f)
    return {spec["project_id"]: spec for spec in all_specs}


def main() -> None:
    ingested_at = datetime.now(timezone.utc).isoformat()
    specs_by_id = load_specs()

    with OUT_PROJECTS_BULK.open("w", encoding="utf-8") as f_proj, OUT_CHUNKS_BULK.open("w", encoding="utf-8") as f_chk:
        for line in IN_PROJECTS.open("r", encoding="utf-8"):
            row = json.loads(line)
            pid = row["project_id"]

            # Projects index
            spec = specs_by_id.get(pid)
            if not spec:
                print(f"WARNING: No spec found for {pid}, skipping")
                continue
            project_doc = {
                "project_id": pid,
                "title": spec.get("title") or "",
                "full_text": row.get("text") or "",
                "status": spec.get("status"),
                "program": spec.get("program") or [],
                "clients": spec.get("clients") or [],
                "partners": spec.get("partners") or [],
                "collaborators": spec.get("collaborators") or [],
                "team_people": spec.get("team_people") or [],
                "team": spec.get("team"),
                "location_raw": spec.get("location_raw"),
                "city": spec.get("city") or [],
                "country": spec.get("country"),
                "year_raw": spec.get("year_raw"),
                "year_start": spec.get("year_start"),
                "year_end": spec.get("year_end"),
                "is_ongoing": spec.get("is_ongoing") or False,
                "source_file": row.get("source_file"),
                "ingested_at": ingested_at
            }

            f_proj.write(json.dumps({"index": {"_id": pid}}, ensure_ascii=False) + "\n")
            f_proj.write(json.dumps(project_doc, ensure_ascii=False) + "\n")

            # Chunks index
            chunks = chunk_text(project_doc["full_text"])
            for idx, ch in enumerate(chunks):
                chunk_id = f"{pid}#{idx:04d}"
                chunk_doc = {
                    "chunk_id": chunk_id,
                    "project_id": pid,
                    "title": project_doc["title"],
                    "chunk_index": idx,
                    "chunk_text": ch,
                    "status": project_doc["status"],
                    "program": project_doc ["program"],
                    "clients": project_doc["clients"],
                    "partners": project_doc["partners"],
                    "collaborators": project_doc["collaborators"],
                    "team_people": project_doc["team_people"],
                    "team": project_doc["team"],
                    "city": project_doc ["city"],
                    "country": project_doc["country"],
                    "year_start": project_doc["year_start"],
                    "year_end": project_doc["year_end"],
                    "is_ongoing": project_doc["is_ongoing"],
                    "ingested_at": ingested_at
                }
                f_chk.write(json.dumps({"index": {"_id": chunk_id}}, ensure_ascii=False) + "\n")
                f_chk.write(json.dumps(chunk_doc, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
