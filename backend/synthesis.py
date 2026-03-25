import re
from typing import Dict, Generator, List
from pydantic import BaseModel

from planner import QueryPlan
from open_ai_client import OpenAIHTTPClient
from fusion import FusionResult, FusedProject
from structured_retriever import ProjectCard
from vector_retriever import ChunkCard


# - - - - - - - - - - DATA MODELS - - - - - - - - - -
class ContextPrompt(BaseModel):
    system: str
    user: str


class ValidatedAnswer(BaseModel):
    answer: str
    citations: list[str]
    warnings: list[str]


# - - - - - - - - - - CONTEXT BUILDER - - - - - - - - - -
class ContextBuilder:
    _PREAMBLE = (
        "You are a knowledgeable assistant for the architecture firm OMA.\n"
        "Answer the user's request using ONLY the provided evidence.\n"
        "Rules:\n"
        "- The entire answer must not be more than 1500 characters long.\n"
        "- Cite every factual claim with project and/or passage references.\n"
        "- Citation format is strict and must match the evidence verbatim:\n"
        "  [P:<project-slug>] or [C:<project-slug>#<passage-id>]\n"
        "- Multiple citations must be separated by a single space.\n"
        "- NEVER use semicolons (;) or commas to group citations.\n"
        "- The first sentence must be a concise, direct answer to the question without citation.\n"
        "- If necesary, acknowledge the provided evidence may be a subset of all matching projects."
        "- Do not include any follow up question or follow up actions."
    )
    _INTENT_FORMATS: Dict[str, str] = {
        "list": (
            "After the first sentence, provide a bulleted list with the project title or the relevant key metadada.\n"
            "Don't include all project card."
        ),
        "count": (
            "After the first sentence, briefly explain what was counted. Do not include any filters information."
        ),
        "explain": (
            "After the first sentence, keep it concise and lean on [C:...] passage citations for supporting detail.\n"
            "If you use bulleted elements, don't include more than 3."
        ),
        "compare": (
            "After the first sentence, use parallel structure highlighting similarities and differences between projects."
        ),
        "other": (
            "After the first sentence, use the most appropriate structure for the question."
        ),
    }


    # - - - - - Public API - - - - -
    def build(self, *, plan: QueryPlan, fused: FusionResult) -> ContextPrompt:
        return ContextPrompt(
            system=self._build_system(plan),
            user=self._build_user(plan, fused),
        )


    # - - - - - Private & Helpers - - - - -
    def _build_system(self, plan: QueryPlan) -> str:
        fmt = self._INTENT_FORMATS.get(plan.intent, self._INTENT_FORMATS["other"])
        return f"{self._PREAMBLE}\n{fmt}"


    def _build_user(self, plan: QueryPlan, fused: FusionResult) -> str:
        sections: List[str] = []

        # Header
        header_lines = [
            f"\nQUERY: {plan.user_query}",
            f"\nINTENT: {plan.intent}",
            f"\nFILTERS APPLIED: {plan.filters if plan.filters else 'none'}",
        ]
        if fused.total is not None:
            header_lines.append(f"\nTOTAL MATCHING PROJECTS: {fused.total}")
        sections.append("".join(header_lines))

        # Warnings
        if fused.warnings:
            sections.append(f"WARNINGS: {'; '.join(fused.warnings)}")

        # Evidence blocks
        if fused.projects:
            blocks: List[str] = []
            for fp in fused.projects:
                blocks.append(self._render_project(fp))
            sections.append("EVIDENCE:\n" + "\n---\n".join(blocks))
        else:
            sections.append("EVIDENCE:\nNo project evidence was retrieved.")

        return "\n".join(sections)


    def _render_project(self, fp: FusedProject) -> str:
        parts: List[str] = [self._render_card(fp.card)]
        for chunk in fp.chunks:
            parts.append(self._render_chunk(chunk))
        return "\n".join(parts)


    def _render_card(self, card: ProjectCard) -> str:
        lines: List[str] = [f"Project: {card.title} [{card.citation_id}]"]
        if card.status:
            lines.append(f"  status: {card.status}")
        if card.program:
            lines.append(f"  program: {', '.join(card.program)}")
        if card.city or card.country:
            loc_parts = []
            if card.city:
                loc_parts.append(", ".join(card.city))
            if card.country:
                loc_parts.append(", ".join(card.country))
            lines.append(f"  location: {', '.join(loc_parts)}")
        year = self._format_year(card)
        if year:
            lines.append(f"  year: {year}")
        if card.partners:
            lines.append(f"  partners: {', '.join(card.partners)}")
        if card.clients:
            lines.append(f"  clients: {', '.join(card.clients)}")
        return "\n".join(lines)


    def _render_chunk(self, chunk: ChunkCard) -> str:
        return f"Passage [{chunk.citation_id}]:\n  {chunk.chunk_text}"


    def _format_year(self, card: ProjectCard) -> str:
        if card.year_start and card.year_end:
            if card.year_start == card.year_end:
                return str(card.year_start)
            return f"{card.year_start}-{card.year_end}"
        if card.year_start:
            return f"{card.year_start}-"
        if card.year_end:
            return f"-{card.year_end}"
        return ""


# - - - - - - - - - - SYNTHESIZER - - - - - - - - - -
class Synthesizer:
    def __init__(self) -> None:
        self._llm = OpenAIHTTPClient(model="gpt-4.1-mini") # gpt-4.1-mini | gpt-5-mini

    @property
    def last_usage(self) -> dict:
        return self._llm.last_usage

    def synthesize(self, prompt: ContextPrompt) -> str:
        return self._llm.chat(system=prompt.system, user=prompt.user)

    def synthesize_stream(self, prompt: ContextPrompt) -> Generator[str, None, None]:
        yield from self._llm.chat_stream(system=prompt.system, user=prompt.user)


# - - - - - - - - - - VALIDATOR - - - - - - - - - -
class Validator:
    _CITATION_RE = re.compile(r'\[(P:[^\]]+|C:[^\]]+)\]')
    _MIN_SUBSTANTIVE_LEN = 20

    def validate(self, *, answer: str, fused: FusionResult) -> ValidatedAnswer:
        # Build valid citation ID set from FusionResult
        valid_ids: set[str] = set()
        for fp in fused.projects:
            valid_ids.add(fp.card.citation_id)
            for chunk in fp.chunks:
                valid_ids.add(chunk.citation_id)

        # Extract all citation IDs from the answer
        found_ids = self._CITATION_RE.findall(answer)

        verified: list[str] = []
        hallucinated: list[str] = []
        for cid in found_ids:
            if cid in valid_ids:
                verified.append(cid)
            else:
                hallucinated.append(cid)

        # Deduplicate while preserving order
        verified = list(dict.fromkeys(verified))

        # Collect warnings
        warnings: list[str] = []
        if len(answer.strip()) < self._MIN_SUBSTANTIVE_LEN:
            warnings.append("Empty or degenerate answer")
        for cid in hallucinated:
            warnings.append(f"Hallucinated citation: [{cid}]")
        if len(answer.strip()) >= self._MIN_SUBSTANTIVE_LEN and not found_ids:
            warnings.append("Answer contains no citations")

        return ValidatedAnswer(answer=answer, citations=verified, warnings=warnings)
