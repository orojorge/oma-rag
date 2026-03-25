from pathlib import Path
import json
import re
from bs4 import BeautifulSoup

IN_DIR = Path("data/oma/projects")
OUT_FILE = Path("artifacts/projects.jsonl")

_ws = re.compile(r"\s+")


def clean(s: str) -> str:
    return _ws.sub(" ", (s or "").replace("\u00a0", " ")).strip()


def rich_text(el) -> str:
    if not el:
        return ""
    txt = el.get_text("\n", strip=True)
    lines = [clean(x) for x in txt.splitlines()]
    return "\n\n".join(x for x in lines if x)


def parse_team(row) -> dict | None:
    container = row.select_one(".meta-data__expanded-content") or row

    def parse_roles(scope):
        roles = []
        for r in scope.select(".meta-data__roles"):
            title_el = r.select_one(".meta-data__roles__title")
            title = clean(title_el.get_text(" ", strip=True)) if title_el else ""
            people = [clean(li.get_text(" ", strip=True)) for li in r.select("li")]
            people = [p for p in people if p]
            if title or people:
                roles.append({"role": title or None, "people": people})
        return roles

    def parse_people_list(scope):
        people = [clean(li.get_text(" ", strip=True)) for li in scope.select(".meta-data__people-list li")]
        return [p for p in people if p]

    phases = container.select(".meta-data__phase")
    if phases:
        out_phases = []
        for ph in phases:
            ph_title_el = ph.select_one(".meta-data__phase__title")
            ph_title = clean(ph_title_el.get_text(" ", strip=True)) if ph_title_el else None
            ph_roles = parse_roles(ph)
            ph_people = parse_people_list(ph)
            if ph_title or ph_roles or ph_people:
                out_phases.append(
                    {
                        "phase": ph_title,
                        "roles": ph_roles or None,
                        "people": ph_people or None,
                    }
                )
        return {"phases": out_phases} if out_phases else None

    roles = parse_roles(container)
    people_list = parse_people_list(container)

    if not roles and not people_list:
        return None
    return {"roles": roles or None, "people": people_list or None}


def parse_collaborators(row) -> list | None:
    container = row.select_one(".meta-data__expanded-content") or row

    out = []
    for block in container.select(".meta-data__collaborator"):
        role_el = block.select_one(".meta-data__collaborator-role")
        role = clean(role_el.get_text(" ", strip=True)) if role_el else None
        names = []
        # Case 1: multiple linked / span names
        for el in block.select(
            ".meta-data__collaborator-title a, "
            ".meta-data__collaborator-title span, "
            ".meta-data__collaborator-heading__role-people a, "
            ".meta-data__collaborator-heading__role-people span"
        ):
            txt = clean(el.get_text(" ", strip=True))
            if txt:
                names.append(txt)
        # Case 2: plain text fallback (comma-separated)
        if not names:
            text_el = (
                block.select_one(".meta-data__collaborator-title")
                or block.select_one(".meta-data__collaborator-heading__role-people")
            )
            if text_el:
                raw = clean(text_el.get_text(" ", strip=True))
                if raw:
                    parts = [clean(p) for p in raw.split(",")]
                    names.extend(p for p in parts if p)
        if role or names:
            out.append(
                {
                    "role": role,
                    "names": names or None,
                }
            )

    if not out:
        items = [clean(li.get_text(" ", strip=True)) for li in container.select("li")]
        items = [x for x in items if x]
        if items:
            return [{"names": [x]} for x in items]

    return out or None


def parse_specs(soup) -> dict:
    specs = {}
    for row in soup.select(".meta-data__row"):
        k_el = row.select_one(".meta-data__heading")
        if not k_el:
            continue
        key = clean(k_el.get_text(" ", strip=True))
        if not key:
            continue

        key_l = key.lower()

        # Special structured parsing for expanded sections
        if key_l == "team":
            val = parse_team(row)
            if val is not None:
                specs[key] = val
            continue

        if key_l == "collaborators":
            val = parse_collaborators(row)
            if val is not None:
                specs[key] = val
            continue

        # Default: capture raw text (keeps any uncommon fields)
        v_el = row.select_one(".meta-data__data")
        if v_el:
            val = clean(v_el.get_text(" ", strip=True))
            if val:
                specs[key] = val
            continue

        # If there's no .meta-data__data but there is expanded content, store its text
        expanded = row.select_one(".meta-data__expanded-content")
        if expanded:
            val = clean(expanded.get_text(" ", strip=True))
            if val:
                specs[key] = val

    return specs


def parse_page(html_path: Path, project_id: str) -> dict:
    soup = BeautifulSoup(html_path.read_text(encoding="utf-8", errors="replace"), "html.parser")

    title_el = soup.select_one("h1.project-page__heading")
    title = clean(title_el.get_text(" ", strip=True)) if title_el else project_id

    short_desc = rich_text(soup.select_one(".project-page__description .rich-text"))
    long_desc  = rich_text(soup.select_one("#long-description .rich-text"))
    text = "\n\n".join(x for x in [short_desc, long_desc] if x)
    specs = parse_specs(soup)

    if not text or not specs:
        print(f"! {project_id} not fully parsed")
        
    return {
        "project_id": project_id,
        "title": title,
        "text": text,
        "specs": specs,
        "source_file": str(html_path),
    }


def main() -> None:
    projects = []
    for project_dir in IN_DIR.iterdir():
        if not project_dir.is_dir():
            continue
        html_path = project_dir / "page.html"
        if not html_path.exists():
            continue
        projects.append(parse_page(html_path, project_dir.name))

    with OUT_FILE.open("w", encoding="utf-8") as f:
        for p in projects:
            f.write(json.dumps(p, ensure_ascii=False) + "\n")

    print(f"Wrote {len(projects)} projects → {OUT_FILE}")


if __name__ == "__main__":
    main()
