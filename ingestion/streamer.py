import os
import argparse
import json
from dataclasses import dataclass
import requests
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

PROJECTS_FILE = Path("artifacts/bulk_projects.ndjson")
CHUNKS_FILE = Path("artifacts/bulk_chunks_w_embeddings.ndjson")

PROJECTS_INDEX = "projects_current"
CHUNKS_INDEX = "chunks_current"
BATCH_DOCS = 50
RECREATE_CHUNKS_INDEX = True
RECREATE_PROJECTS_INDEX = True
EMBEDDING_DIM = 1536


@dataclass
class Target:
    name: str
    endpoint: str
    session: requests.Session


def make_local_target() -> Target:
    endpoint = os.getenv("OPENSEARCH_HOST")
    return Target("local", endpoint, requests.Session())


def make_aws_target() -> Target:
    import boto3
    from requests_aws4auth import AWS4Auth

    endpoint = os.environ["OPENSEARCH_HOST"]
    region = "eu-central-1"
    service = "es"

    creds = boto3.Session().get_credentials().get_frozen_credentials()
    auth = AWS4Auth(creds.access_key, creds.secret_key, region, service, session_token=creds.token)

    session = requests.Session()
    session.auth = auth
    return Target("aws", endpoint, session)


def _request(target: Target, method: str, path: str, body: dict | None = None):
    resp = target.session.request(
        method,
        f"{target.endpoint}{path}",
        json=body,
        headers={"Content-Type": "application/json"} if body is not None else {},
    )
    resp.raise_for_status()
    return resp.json()


def _exists(target: Target, path: str) -> bool:
    resp = target.session.head(f"{target.endpoint}{path}")
    if resp.status_code == 404:
        return False
    resp.raise_for_status()
    return True


def delete_index(target: Target, index_name: str) -> None:
    if _exists(target, f"/{index_name}"):
        _request(target, "DELETE", f"/{index_name}")


def create_chunks_index(target: Target) -> None:
    settings = {
        "index": {
            "knn": True,
            "knn.algo_param.ef_search": 100,
        }
    }
    mappings = {
        "properties": {
            "chunk_id": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
            "chunk_index": {"type": "long"},
            "chunk_text": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
            "city": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
            "clients": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
            "collaborators": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
            "country": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
            "embedding": {"type": "knn_vector", "dimension": EMBEDDING_DIM},
            "ingested_at": {"type": "date"},
            "is_ongoing": {"type": "boolean"},
            "partners": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
            "program": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
            "project_id": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
            "status": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
            "team_people": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
            "title": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
            "year_end": {"type": "long"},
            "year_start": {"type": "long"},
        }
    }

    if _exists(target, f"/{CHUNKS_INDEX}"):
        _request(target, "DELETE", f"/{CHUNKS_INDEX}")

    _request(target, "PUT", f"/{CHUNKS_INDEX}", {"settings": settings, "mappings": mappings})


def bulk_post(target: Target, payload: str):
    resp = target.session.post(
        f"{target.endpoint}/_bulk",
        data=payload.encode("utf-8"),
        headers={"Content-Type": "application/x-ndjson"},
    )
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        print("BULK HTTP ERROR:", resp.status_code, resp.text[:2000])
        raise

    out = resp.json()
    if out.get("errors"):
        for item in out.get("items", []):
            action = next(iter(item.values()))
            if "error" in action:
                print("BULK ITEM ERROR:", action["error"])
                break


def load(target: Target, path: str, index_name: str):
    buf, n = [], 0
    with open(path, "r", encoding="utf-8") as f:
        while True:
            a = f.readline()
            if not a:
                break
            d = f.readline()
            if not d:
                raise SystemExit(f"Odd number of lines in {path}")

            action = json.loads(a)
            op = next(iter(action))
            meta = action[op]

            if isinstance(meta, str):
                meta = {"_id": meta}

            meta["_index"] = index_name
            action[op] = meta

            buf += [json.dumps(action, ensure_ascii=False), d.rstrip("\n")]
            n += 1
            if n % BATCH_DOCS == 0:
                bulk_post(target, "\n".join(buf) + "\n")
                buf.clear()
                print(f"[{target.name}] {index_name}: {n}")

    if buf:
        bulk_post(target, "\n".join(buf) + "\n")
    print(f"[{target.name}] {index_name}: DONE {n}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream ndjson indices to OpenSearch.")
    parser.add_argument(
        "--target",
        choices=["local", "aws"],
        action="append",
        required=True,
        help="Destination target (repeatable: --target local --target aws).",
    )
    args = parser.parse_args()

    targets: list[Target] = []
    if "local" in args.target:
        targets.append(make_local_target())
    if "aws" in args.target:
        targets.append(make_aws_target())

    for t in targets:
        print(f"--- target: {t.name} ({t.endpoint}) ---")
        if RECREATE_PROJECTS_INDEX:
            delete_index(t, PROJECTS_INDEX)
        if RECREATE_CHUNKS_INDEX:
            create_chunks_index(t)
        load(t, PROJECTS_FILE, PROJECTS_INDEX)
        load(t, CHUNKS_FILE, CHUNKS_INDEX)


if __name__ == "__main__":
    main()
