import json
import os
from typing import Iterable, List, Tuple
from openai import OpenAI
from tqdm import tqdm
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

INPUT_PATH = Path("artifacts/bulk_chunks.ndjson")
OUTPUT_PATH = Path("artifacts/bulk_chunks_w_embeddings.ndjson")

TEXT_FIELD = "chunk_text"
EMBEDDING_FIELD = "embedding"
MODEL = "text-embedding-3-small"
BATCH_SIZE = 100
CLIENT = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def read_ndjson(path: Path) -> Iterable[dict]:
    with path.open("r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON on line {line_num}") from e


def write_ndjson(path: Path, records: Iterable[dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def batched(items: List[dict], size: int) -> Iterable[List[dict]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def embed_batch(batch: List[dict], output_records: List[dict]) -> None:
    texts = [r[TEXT_FIELD] for r in batch]

    response = CLIENT.embeddings.create(
        model=MODEL,
        input=texts,
    )

    for record, emb in zip(batch, response.data):
        record[EMBEDDING_FIELD] = emb.embedding
        output_records.append(record)


def embed_batch_in_place(
    batch: List[Tuple[int, dict]],
    output_records: list[dict],
) -> None:
    texts = [rec[TEXT_FIELD] for _, rec in batch]

    response = CLIENT.embeddings.create(
        model=MODEL,
        input=texts,
    )

    for (out_idx, _rec), emb in zip(batch, response.data):
        output_records[out_idx][EMBEDDING_FIELD] = emb.embedding


def main() -> None:
    output_records: list[dict] = []
    buffer: list[tuple[int, dict]] = []

    for record in tqdm(read_ndjson(INPUT_PATH), desc="Processing records"):
        out_idx = len(output_records)
        output_records.append(record)
        text = record.get(TEXT_FIELD)
        if not isinstance(text, str) or not text.strip():
            continue

        buffer.append((out_idx, record))

        if len(buffer) >= BATCH_SIZE:
            embed_batch_in_place(buffer, output_records)
            buffer.clear()

    if buffer:
        embed_batch_in_place(buffer, output_records)

    write_ndjson(OUTPUT_PATH, output_records)


if __name__ == "__main__":
    main()
