"""
Retrieval and Grounding layer.
Given a natural-language question, returns a ContextPack of ranked
evidence snippets and linked entities/claims.
Uses hybrid search: keyword matching + TF-IDF similarity.
"""
from __future__ import annotations
import re
from collections import defaultdict
from typing import Any
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from graph import MemoryGraph
from schema import Entity, Claim, Evidence, ContextPack, ClaimType
class MemoryRetriever:
    """Hybrid search over the memory graph."""
    def __init__(self, graph: MemoryGraph):
        self.graph = graph
        self._tfidf: TfidfVectorizer | None = None
        self._entity_vectors: np.ndarray | None = None
        self._entity_ids: list[str] = []
        self._claim_vectors: np.ndarray | None = None
        self._claim_ids: list[str] = []
        self._evidence_vectors: np.ndarray | None = None
        self._evidence_ids: list[str] = []
    # -------------------------------------------------------------------
    # Indexing
    # -------------------------------------------------------------------
    def build_index(self) -> None:
        """Build TF-IDF index over entities, claims, and evidence."""
        print("Building retrieval index …")
        # Collect text representations
        entity_texts: list[str] = []
        self._entity_ids = []
        entities = self.graph.get_entities(limit=10000)
        for e in entities:
            text = f"{e.name} {' '.join(e.aliases)} {e.entity_type.value}"
            if e.properties:
                text += " " + " ".join(str(v) for v in e.properties.values())
            entity_texts.append(text)
            self._entity_ids.append(e.id)
        claim_texts: list[str] = []
        self._claim_ids = []
        claims = self.graph.get_all_claims(limit=50000)
        for c in claims:
            text = f"{c.predicate} {c.object_value or ''} {c.claim_type.value}"
            claim_texts.append(text)
            self._claim_ids.append(c.id)
        evidence_texts: list[str] = []
        self._evidence_ids = []
        evidence_rows = self.graph.conn.execute("SELECT * FROM evidence").fetchall()
        for row in evidence_rows:
            ev = self.graph._row_to_evidence(row)
            evidence_texts.append(ev.excerpt)
            self._evidence_ids.append(ev.id)
        # Build unified TF-IDF
        all_texts = entity_texts + claim_texts + evidence_texts
        if not all_texts:
            print("  No data to index.")
            return
        self._tfidf = TfidfVectorizer(
            max_features=5000,
            stop_words="english",
            ngram_range=(1, 2),
            min_df=1,
            max_df=0.95,
        )
        all_vectors = self._tfidf.fit_transform(all_texts)
        n_ent = len(entity_texts)
        n_claim = len(claim_texts)
        self._entity_vectors = all_vectors[:n_ent]
        self._claim_vectors = all_vectors[n_ent : n_ent + n_claim]
        self._evidence_vectors = all_vectors[n_ent + n_claim :]
        print(f"  Indexed {n_ent} entities, {n_claim} claims, {len(evidence_texts)} evidence")
    # -------------------------------------------------------------------
    # Search
    # -------------------------------------------------------------------
    def search(
        self,
        question: str,
        top_k: int = 10,
        include_historical: bool = False,
    ) -> ContextPack:
        """
        Search the memory graph for relevant context.
        Returns a ContextPack with:
          - Ranked entities
          - Related claims (with evidence)
          - Evidence snippets
          - Optional summary
        """
        if self._tfidf is None:
            self.build_index()
        # --- Step 1: Find candidate entities ---
        entity_scores = self._score_entities(question)
        claim_scores = self._score_claims(question)
        evidence_scores = self._score_evidence(question)
        # --- Step 2: Expand via graph neighbors ---
        top_entity_ids = [eid for eid, _ in entity_scores[:top_k]]
        # Add entities connected via top claims
        for cid, _ in claim_scores[:top_k]:
            claim = self._get_claim_by_id(cid)
            if claim:
                if claim.subject_id not in top_entity_ids:
                    top_entity_ids.append(claim.subject_id)
                if claim.object_id and claim.object_id not in top_entity_ids:
                    top_entity_ids.append(claim.object_id)
        # --- Step 3: Collect entities ---
        result_entities: list[Entity] = []
        seen_entity_ids: set[str] = set()
        for eid in top_entity_ids[:top_k * 2]:
            if eid in seen_entity_ids:
                continue
            entity = self.graph.get_entity(eid)
            if entity:
                result_entities.append(entity)
                seen_entity_ids.add(eid)
        # --- Step 4: Collect claims for those entities ---
        result_claims: list[Claim] = []
        seen_claim_ids: set[str] = set()
        for entity in result_entities:
            entity_claims = self.graph.get_claims_for_entity(
                entity.id, current_only=not include_historical
            )
            for claim in entity_claims:
                if claim.id not in seen_claim_ids:
                    result_claims.append(claim)
                    seen_claim_ids.add(claim.id)
        # Add top-scoring claims not already included
        for cid, _ in claim_scores[:top_k]:
            if cid not in seen_claim_ids:
                claim = self._get_claim_by_id(cid)
                if claim:
                    result_claims.append(claim)
                    seen_claim_ids.add(cid)
        # --- Step 5: Collect evidence ---
        result_evidence: list[Evidence] = []
        seen_evidence_ids: set[str] = set()
        # Evidence from claims
        for claim in result_claims:
            for ev in self.graph.get_evidence_for_claim(claim.id):
                if ev.id not in seen_evidence_ids:
                    result_evidence.append(ev)
                    seen_evidence_ids.add(ev.id)
        # Top-scoring evidence
        for evid, _ in evidence_scores[:top_k]:
            if evid not in seen_evidence_ids:
                row = self.graph.conn.execute(
                    "SELECT * FROM evidence WHERE id = ?", (evid,)
                ).fetchone()
                if row:
                    result_evidence.append(self.graph._row_to_evidence(row))
                    seen_evidence_ids.add(evid)
        # --- Step 6: Rank and prune ---
        result_entities = result_entities[:top_k]
        result_claims = sorted(
            result_claims, key=lambda c: c.confidence, reverse=True
        )[:top_k * 2]
        result_evidence = result_evidence[:top_k * 3]
        return ContextPack(
            question=question,
            entities=result_entities,
            claims=result_claims,
            evidence=result_evidence,
        )
    def _score_entities(self, query: str) -> list[tuple[str, float]]:
        """Score entities against query using TF-IDF similarity + keyword bonus."""
        if not self._tfidf or self._entity_vectors is None or len(self._entity_ids) == 0:
            return []
        query_vec = self._tfidf.transform([query])
        sims = cosine_similarity(query_vec, self._entity_vectors).flatten()
        # Keyword bonus: exact match in name
        query_lower = query.lower()
        scores: list[tuple[str, float]] = []
        entities = self.graph.get_entities(limit=10000)
        entity_map = {e.id: e for e in entities}
        for i, eid in enumerate(self._entity_ids):
            score = float(sims[i])
            entity = entity_map.get(eid)
            if entity:
                # Keyword bonus
                if query_lower in entity.name.lower():
                    score += 0.3
                for alias in entity.aliases:
                    if query_lower in alias.lower():
                        score += 0.1
            scores.append((eid, score))
        scores.sort(key=lambda x: x[1], reverse=True)
        return scores
    def _score_claims(self, query: str) -> list[tuple[str, float]]:
        """Score claims against query."""
        if not self._tfidf or self._claim_vectors is None or len(self._claim_ids) == 0:
            return []
        query_vec = self._tfidf.transform([query])
        sims = cosine_similarity(query_vec, self._claim_vectors).flatten()
        scores = [(cid, float(sims[i])) for i, cid in enumerate(self._claim_ids)]
        scores.sort(key=lambda x: x[1], reverse=True)
        return scores
    def _score_evidence(self, query: str) -> list[tuple[str, float]]:
        """Score evidence against query."""
        if not self._tfidf or self._evidence_vectors is None or len(self._evidence_ids) == 0:
            return []
        query_vec = self._tfidf.transform([query])
        sims = cosine_similarity(query_vec, self._evidence_vectors).flatten()
        scores = [(evid, float(sims[i])) for i, evid in enumerate(self._evidence_ids)]
        scores.sort(key=lambda x: x[1], reverse=True)
        return scores
    def _get_claim_by_id(self, claim_id: str) -> Claim | None:
        row = self.graph.conn.execute(
            "SELECT * FROM claims WHERE id = ?", (claim_id,)
        ).fetchone()
        return self.graph._row_to_claim(row) if row else None
    # -------------------------------------------------------------------
    # Context pack formatting
    # -------------------------------------------------------------------
    @staticmethod
    def format_context_pack(pack: ContextPack) -> str:
        """Format a context pack as human-readable text with citations."""
        lines = [f"Question: {pack.question}", ""]
        if pack.entities:
            lines.append("=== Relevant Entities ===")
            for e in pack.entities:
                lines.append(f"  [{e.entity_type.value}] {e.name}")
                if e.aliases:
                    lines.append(f"    Aliases: {', '.join(e.aliases[:5])}")
                if e.properties:
                    for k, v in list(e.properties.items())[:3]:
                        lines.append(f"    {k}: {v}")
            lines.append("")
        if pack.claims:
            lines.append("=== Related Claims ===")
            for c in pack.claims:
                status = "CURRENT" if c.is_current else "HISTORICAL"
                val = c.object_value or c.object_id or ""
                lines.append(
                    f"  [{status}] {c.predicate}: {val[:80]} "
                    f"(confidence: {c.confidence:.2f}, type: {c.claim_type.value})"
                )
            lines.append("")
        if pack.evidence:
            lines.append("=== Supporting Evidence ===")
            for i, ev in enumerate(pack.evidence):
                lines.append(f"  [{i+1}] Source: {ev.source_id}")
                lines.append(f"      URL: {ev.source_url}")
                lines.append(f"      Excerpt: {ev.excerpt[:200]}")
                if ev.timestamp:
                    lines.append(f"      Time: {ev.timestamp.isoformat()}")
                lines.append("")
        return "\n".join(lines)