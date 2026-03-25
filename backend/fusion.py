from collections import defaultdict
from typing import Dict, List, Optional
from pydantic import BaseModel, Field

from planner import QueryPlan
from structured_retriever import ProjectCard, StructuredRetrievalResult
from vector_retriever import ChunkCard, VectorRetrievalResult


# - - - - - - - - - - DATA MODELS - - - - - - - - - -

class FusedProject(BaseModel):
    card: ProjectCard
    chunks: List[ChunkCard] = Field(default_factory=list)


class FusionResult(BaseModel):
    projects: List[FusedProject] = Field(default_factory=list)
    total: Optional[int] = None
    warnings: List[str] = Field(default_factory=list)


# - - - - - - - - - - FUSION LAYER - - - - - - - - - -

class FusionLayer:
    def fuse(
        self,
        plan: QueryPlan,
        structured: StructuredRetrievalResult | None,
        vector: VectorRetrievalResult | None,
    ) -> FusionResult:
        # 1. Early return
        if structured is None and vector is None:
            return FusionResult()

        warnings: List[str] = []

        # 2. Index ProjectCards by project_id
        cards: Dict[str, ProjectCard] = {}
        structured_order: List[str] = []
        if structured is not None:
            for c in structured.projects:
                cards[c.project_id] = c
                structured_order.append(c.project_id)
            warnings.extend(structured.warnings)

        # 3. Group chunks by project_id, sorted by score desc then chunk_index asc
        chunks_by_project: Dict[str, List[ChunkCard]] = defaultdict(list)
        if vector is not None:
            for ch in vector.chunks:
                chunks_by_project[ch.project_id].append(ch)
            warnings.extend(vector.warnings)

        for pid in chunks_by_project:
            chunks_by_project[pid].sort(
                key=lambda c: (-(c.score if c.score is not None else 0.0), c.chunk_index or 0)
            )

        # 4. Cap chunks per project
        cpp = max(1, int(plan.chunks_per_project or 1))
        for pid in chunks_by_project:
            chunks_by_project[pid] = chunks_by_project[pid][:cpp]

        # 5. Assemble FusedProject list
        all_pids = list(dict.fromkeys(list(cards.keys()) + list(chunks_by_project.keys())))

        fused: List[FusedProject] = []
        for pid in all_pids:
            card = cards.get(pid)
            if card is None:
                # Synthesize minimal card from chunk metadata
                sample = chunks_by_project[pid][0]
                card = ProjectCard(
                    project_id=pid,
                    title=sample.title,
                    citation_id=f"P:{pid}",
                )
                warnings.append(f"Synthesized ProjectCard for {pid!r} (no structured result)")

            fused.append(FusedProject(
                card=card,
                chunks=chunks_by_project.get(pid, []),
            ))

        # 6. Sort projects: chunks-first by best score, then structured-only in original order
        structured_pos = {pid: i for i, pid in enumerate(structured_order)}

        def _sort_key(fp: FusedProject) -> tuple:
            if fp.chunks:
                best = fp.chunks[0].score if fp.chunks[0].score is not None else 0.0
                return (0, -best)
            return (1, structured_pos.get(fp.card.project_id, len(structured_pos)))

        fused.sort(key=_sort_key)

        # 7. Apply plan.limit
        fused = fused[: plan.limit]

        # 8. Propagate total and warnings
        total = structured.total if structured is not None else None

        return FusionResult(projects=fused, total=total, warnings=warnings)
