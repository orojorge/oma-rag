import json
import re
from dataclasses import replace
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from planner import QueryPlan


class Normalizer:
    SUPPORTED_MODES = {"structured", "hybrid"}
    _SUPPORTED_KEYS = {"status", "program", "city", "country"}

    def __init__(self, vocabulary_path: str) -> None:
        self.vocabulary_path = Path(vocabulary_path)
        self._vocab_raw: Dict[str, Any] = {}
        self._lookups: Dict[str, Dict[str, Dict[str, str]]] = {}
        self._load_vocab()


    # - - - - - Public API - - - - -
    def normalize(self, plan: QueryPlan) -> QueryPlan:
        mode = getattr(plan, "mode", None)
        if mode not in self.SUPPORTED_MODES:
            return plan

        filters = getattr(plan, "filters", None) or {}
        changes: List[Dict[str, Any]] = []
        out_filters = dict(filters)

        # Normalize supported keys using per-key methods.
        out_filters = self._norm_city(out_filters, changes)
        out_filters = self._norm_country(out_filters, changes)
        out_filters = self._norm_status(out_filters, changes)
        out_filters = self._norm_program(out_filters, changes)

        report = {
            "changes": changes,
        }

        # Create a new plan instance with updated filters + appended notes.
        return self._copy_plan(
            plan,
            updates={
                "filters": out_filters,
                "notes": self._augment_notes(getattr(plan, "notes", None), report),
            },
        )


    # - - - - - Vocabulary loading - - - - -
    def _load_vocab(self) -> None:
        raw = json.loads(self.vocabulary_path.read_text(encoding="utf-8"))
        self._vocab_raw = raw

        global_aliases = raw.get("aliases", {})
        if not isinstance(global_aliases, dict):
            global_aliases = {}

        lookups: Dict[str, Dict[str, Dict[str, str]]] = {}

        for key in self._SUPPORTED_KEYS:
            values = raw.get(key, [])
            if not isinstance(values, list):
                values = []

            canon_map: Dict[str, str] = {}
            for val in values:
                if isinstance(val, str) and val.strip():
                    canon_map[self._keynorm(val)] = val

            alias_map: Dict[str, str] = {}
            if key in ("status", "program", "city", "country"):
                for canon, aliases in global_aliases.items():
                    if not isinstance(canon, str) or not canon.strip():
                        continue
                    if not isinstance(aliases, list):
                        continue

                    canon_norm = self._keynorm(canon)

                    if canon_norm not in canon_map:
                        continue

                    for a in aliases:
                        if not isinstance(a, str) or not a.strip():
                            continue
                        alias_map[self._keynorm(a)] = canon_map[canon_norm]

            lookups[key] = {"canon": canon_map, "alias": alias_map}

        self._lookups = lookups


    # - - - - - Per-key normalizers - - - - -
    def _norm_city(self, filters: Dict[str, Any], changes: List[Dict[str, Any]]) -> Dict[str, Any]:
        return self._norm_scalar_key("city", filters, changes)

    def _norm_country(self, filters: Dict[str, Any], changes: List[Dict[str, Any]]) -> Dict[str, Any]:
        return self._norm_scalar_key("country", filters, changes)

    def _norm_status(self, filters: Dict[str, Any], changes: List[Dict[str, Any]]) -> Dict[str, Any]:
        return self._norm_scalar_key("status", filters, changes)
    
    def _norm_program(self, filters: Dict[str, Any], changes: List[Dict[str, Any]]) -> Dict[str, Any]:
        return self._norm_scalar_key("program", filters, changes)


    # - - - - - Core mapping helpers - - - - -
    def _norm_scalar_key(
        self,
        key: str,
        filters: Dict[str, Any],
        changes: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        raw = filters.get(key)

        # Accept str or list[str]
        if isinstance(raw, str):
            values = [raw]
            is_list = False
        elif isinstance(raw, list) and all(isinstance(v, str) for v in raw):
            values = raw
            is_list = True
        else:
            filters.pop(key, None)
            return filters

        # Normalize each value
        normalized: List[str] = []
        for v in values:
            v_clean = v.strip()
            if not v_clean:
                continue

            canon, rule = self._map_value(key, v_clean)
            if canon is None:
                continue

            if canon != v_clean:
                changes.append({"k": key, "from": v_clean, "to": canon, "rule": rule})

            normalized.append(canon)

        # Decide whether to keep or drop the key
        if not normalized:
            filters.pop(key, None)
            return filters

        # Preserve original shape
        filters[key] = normalized if is_list else normalized[0]
        return filters

    def _map_value(self, key: str, raw_value: str) -> Tuple[Optional[str], str]:
        norm = self._keynorm(raw_value)
        lookup = self._lookups.get(key, {})
        canon_map: Dict[str, str] = lookup.get("canon", {}) or {}
        alias_map: Dict[str, str] = lookup.get("alias", {}) or {}

        # 1) exact match against canonical values (case/space-insensitive)
        if norm in canon_map:
            return canon_map[norm], "exact"

        # 2) alias match (case/space-insensitive)
        if norm in alias_map:
            return alias_map[norm], "alias"

        return None, "none"


    # - - - - - Observability - - - - -
    def _augment_notes(self, existing: Optional[str], report: Dict[str, Any]) -> str:
        base = (existing or "").strip()
        payload = json.dumps(report, separators=(",", ":"), sort_keys=True)
        if not base:
            return f"norm={payload}"
        # Keep consistent separator; callers already use " | " elsewhere.
        return f"{base} | norm={payload}"
    
    def _copy_plan(self, plan: QueryPlan, updates: dict[str, Any]) -> QueryPlan:
        return plan.model_copy(update=updates)


    # - - - - - Helpers - - - - -
    _space_re = re.compile(r"\s+")

    def _keynorm(self, s: str) -> str:
        # Casefold + collapse whitespace + strip
        s2 = self._space_re.sub(" ", s).strip().casefold()
        return s2


if __name__ == "__main__":
    normalizer = Normalizer("utils/vocabulary_w_alias.json")