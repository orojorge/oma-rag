import json
from pathlib import Path
import unicodedata

ARTIFACTS_DIR = Path("artifacts")
REVIEWED_SPECS = ARTIFACTS_DIR / "specs_reviewed.json"
DEFAULT_SPECS = ARTIFACTS_DIR / "specs.json"
IN_PATH = REVIEWED_SPECS if REVIEWED_SPECS.exists() else DEFAULT_SPECS
OUT_PATH_BACKEND = Path("../backend/aliases.json")

KEYS = ("status", "program", "city", "country")


def _iter_values(v):
    if v is None:
        return
    if isinstance(v, list):
        for x in v:
            if isinstance(x, str):
                s = x.strip()
                if s:
                    yield s
    elif isinstance(v, str):
        s = v.strip()
        if s:
            yield s


def vocab_from_json(path: Path) -> dict[str, list[str]]:
    acc = {k: set() for k in KEYS}

    with path.open("r", encoding="utf-8") as f:
        specs = json.load(f)

    for obj in specs:
        for k in KEYS:
            for val in _iter_values(obj.get(k)):
                acc[k].add(val)

    return {k: sorted(acc[k]) for k in KEYS}


# - - - - - - - - - - Aliases - - - - - - - - - -

def _normalize(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.lower().replace(".", "").strip()


def _build_aliases(values: list[str]) -> dict[str, list[str]]:
    norm_map: dict[str, set[str]] = {}

    for v in values:
        norm = _normalize(v)
        norm_map.setdefault(norm, set()).add(v)

    # choose a stable canonical form per group
    aliases: dict[str, list[str]] = {}
    for variants in norm_map.values():
        canonical = sorted(variants, key=len)[0]
        others = sorted(v for v in variants if v != canonical)
        if others:
            aliases[canonical] = others

    return aliases


def add_city_country_aliases(vocab: dict) -> dict:
    merged = {}
    for key in KEYS:
        merged.update(_build_aliases(vocab.get(key, [])))
    vocab["aliases"] = merged
    return vocab


def main():
    vocab = vocab_from_json(IN_PATH)
    aliases = add_city_country_aliases(vocab)
    with OUT_PATH_BACKEND.open("w", encoding="utf-8") as f:
        json.dump(aliases, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()