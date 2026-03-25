from typing import Any, Dict, List, Optional


def as_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        return s if s else None
    return str(v).strip() or None


def as_str_list(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, list):
        out: List[str] = []
        for x in v:
            s = as_str(x)
            if s:
                out.append(s)
        return out
    s = as_str(v)
    return [s] if s else []


def as_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            return int(s)
        except ValueError:
            return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def as_bool(v: Any) -> Optional[bool]:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, int) and v in (0, 1):
        return bool(v)
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("true", "t", "1", "yes", "y"):
            return True
        if s in ("false", "f", "0", "no", "n"):
            return False
    return None


def extract_total(resp: Dict[str, Any]) -> Optional[int]:
    hits = (resp or {}).get("hits") or {}
    total = hits.get("total")
    if isinstance(total, int):
        return total
    if isinstance(total, dict) and isinstance(total.get("value"), int):
        return int(total["value"])
    return None
