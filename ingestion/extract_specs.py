import json
import re
from pathlib import Path
import pycountry

IN_PATH = Path("artifacts/projects.jsonl")
OUT_PATH = Path("artifacts/specs.json")

YEAR_RE = re.compile(r"(\d{4})")
COUNTRY_ALIASES = {
    "russia", "uk", "usa", "south korea", "north korea", "iran", "syria",
    "vietnam", "taiwan", "hong kong", "macau", "bolivia", "venezuela",
    "tanzania", "laos", "brunei", "moldova", "palestine", "democratic republic of congo",
    "the netherlands", "turkey", "south-korea", "uae", "korea", "kingdom of saudi arabia"
}


def split_csvish(s: str) -> list[str]:
    if not s:
        return []
    return [p.strip() for p in s.replace(" / ", ",").split(",") if p.strip()]


def is_country(name: str) -> bool:
    name = name.strip()
    name_lower = name.lower()
    if name_lower in COUNTRY_ALIASES:
        return True
    if pycountry.countries.get(name=name):
        return True
    if pycountry.countries.get(alpha_2=name.upper()):
        return True
    if pycountry.countries.get(alpha_3=name.upper()):
        return True
    return False


def parse_location(loc: str | None) -> dict:
    loc = loc or ""
    loc_clean = " ".join(loc.replace("\u00a0", " ").split()).strip()
    loc_clean_ = loc_clean.replace("/", ",")
    parts = [p.strip() for p in loc_clean_.split(",") if p.strip()]
    if not parts:
        return {"location_raw": loc_clean, "city": [], "country": []}
    if len(parts) == 1:
        if is_country(parts[0]):
            return {"location_raw": loc_clean, "city": [], "country": [parts[0]]}
        return {"location_raw": loc_clean, "city": [parts[0]], "country": []}
    if is_country(parts[-1]):
        return {"location_raw": loc_clean, "city": parts[:-1], "country": [parts[-1]]}
    return {"location_raw": loc_clean, "city": parts, "country": []}


def parse_year(y: str) -> dict:
    y_clean = " ".join((y or "").split()).lower()
    is_ongoing = "ongoing" in y_clean
    years = [int(m) for m in YEAR_RE.findall(y_clean)]
    if not years:
        return {"year_raw": y, "year_start": None, "year_end": None, "is_ongoing": is_ongoing}
    if len(years) == 1:
        year_end = None if is_ongoing else years[0]
        return {"year_raw": y, "year_start": years[0], "year_end": year_end, "is_ongoing": is_ongoing}
    return {"year_raw": y, "year_start": years[0], "year_end": years[-1], "is_ongoing": is_ongoing}


def parse_collaborators(specs: dict) -> list[str]:
    raw = specs.get("Collaborators") or specs.get("Collaborator") or []
    out: list[str] = []
    if isinstance(raw, str):
        out.extend(split_csvish(raw))
    elif isinstance(raw, list):
        for item in raw:
            if isinstance(item, str):
                out.extend(split_csvish(item))
            elif isinstance(item, dict):
                for v in item.values():
                    if isinstance(v, str):
                        out.extend(split_csvish(v))
                    elif isinstance(v, list):
                        for n in v:
                            out.extend(split_csvish(n))
    elif isinstance(raw, dict):
        for v in raw.values():
            if isinstance(v, str):
                out.extend(split_csvish(v))
    seen = set()
    cleaned = []
    for c in out:
        c = c.strip()
        if c and c not in seen:
            seen.add(c)
            cleaned.append(c)
    return cleaned


def flatten_team_people(team_obj) -> list[str]:
    people = []
    if not isinstance(team_obj, dict):
        return people
    phases = team_obj.get("phases") or []
    for ph in phases:
        for p in (ph.get("roles") or []):
            rp = p.get("people", [])
            for rpp in rp:
                if rpp and isinstance(rpp, str):
                    people.append(rpp.strip())
        for p in (ph.get("people") or []):
            if p and isinstance(p, str):
                people.append(p.strip())
    seen = set()
    out = []
    for p in people:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


def main() -> None:
    all_specs = []

    for line in IN_PATH.open("r", encoding="utf-8"):
        row = json.loads(line)
        pid = row["project_id"]
        specs = row.get("specs") or {}

        clients = []
        if specs.get("Client"):
            clients += split_csvish(specs["Client"])
        if specs.get("Clients"):
            clients += split_csvish(specs["Clients"])
        clients = list(dict.fromkeys([c for c in clients if c]))

        partners = []
        if specs.get("Partner"):
            partners += split_csvish(specs["Partner"])
        if specs.get("Partners"):
            partners += split_csvish(specs["Partners"])
        partners = list(dict.fromkeys([p for p in partners if p]))

        collaborators = parse_collaborators(specs)
        loc = parse_location(specs.get("Location") or "")
        yr = parse_year(specs.get("Year") or "")
        team_obj = specs.get("Team")
        team_people = flatten_team_people(team_obj)
        status = (specs.get("Status") or "").strip() or None

        program = specs.get("Program")
        program_list = split_csvish(program) if isinstance(program, str) else (program or [])
        program_list = [p for p in program_list if isinstance(p, str) and p.strip()]

        spec_doc = {
            "project_id": pid,
            "title": row.get("title") or "",
            "status": status,
            "program": program_list,
            "clients": clients,
            "partners": partners,
            "collaborators": collaborators,
            "team_people": team_people,
            "team": team_obj,
            **loc,
            **yr,
        }

        all_specs.append(spec_doc)

    with OUT_PATH.open("w", encoding="utf-8") as f_out:
        json.dump(all_specs, f_out, ensure_ascii=False, indent=2)

    print(f"Wrote {OUT_PATH}")


if __name__ == "__main__":
    main()
