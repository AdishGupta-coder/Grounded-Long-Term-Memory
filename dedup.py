"""
Deduplication and canonicalization system.
Three levels of dedup:
  1. Artifact dedup — detect near-duplicate issues/messages
  2. Entity canonicalization — merge entities referring to the same real-world thing
  3. Claim dedup — merge repeated statements of the same fact
All merges are logged and reversible via MergeRecord.
"""
from __future__ import annotations
import re
from collections import defaultdict
from datetime import datetime
from schema import Entity, EntityType, Claim, ClaimType, Evidence, MergeRecord
from datetime import timezone

#----------------------------------------------------------------------------
# Text similarity (lightweight, no external deps beyond scikit-learn)
# ---------------------------------------------------------------------------
def to_utc(dt):
    if dt is None:
        return datetime.max.replace(tzinfo=timezone.utc)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)
def _normalize_text(text: str) -> str:
    """Lowercase, strip whitespace, collapse runs of whitespace."""
    return re.sub(r'\s+', ' ', text.lower().strip())
def _jaccard_similarity(a: str, b: str) -> float:
    """Word-level Jaccard similarity between two strings."""
    words_a = set(_normalize_text(a).split())
    words_b = set(_normalize_text(b).split())
    if not words_a or not words_b:
        return 0.0
    intersection = words_a & words_b
    union = words_a | words_b
    return len(intersection) / len(union)
# ---------------------------------------------------------------------------
# 1. Artifact Deduplication
# ---------------------------------------------------------------------------
def dedup_artifacts(
    entities: list[Entity],
    claims: list[Claim],
    similarity_threshold: float = 0.85,
) -> tuple[list[Entity], list[MergeRecord]]:
    """
    Detect near-duplicate issues/PRs based on title similarity.
    Marks duplicates by setting canonical_id on the duplicate entity.
    """
    merge_records: list[MergeRecord] = []
    artifact_types = {EntityType.ISSUE, EntityType.PULL_REQUEST}
    # Group artifacts by type
    artifacts = [e for e in entities if e.entity_type in artifact_types]
    # Compare all pairs (O(n²) but n is small for our corpus)
    merged_ids: set[str] = set()
    for i, a in enumerate(artifacts):
        if a.id in merged_ids:
            continue
        title_a = a.properties.get("title", a.name)
        for j in range(i + 1, len(artifacts)):
            b = artifacts[j]
            if b.id in merged_ids:
                continue
            if a.entity_type != b.entity_type:
                continue
            title_b = b.properties.get("title", b.name)
            sim = _jaccard_similarity(title_a, title_b)
            if sim >= similarity_threshold:
                # Mark b as duplicate of a
                b.canonical_id = a.id
                merged_ids.add(b.id)
                a.aliases.append(b.name)
                merge_records.append(MergeRecord(
                    merge_type="artifact",
                    source_id=b.id,
                    target_id=a.id,
                    reason=f"Title similarity {sim:.2f}: '{title_a[:50]}' ≈ '{title_b[:50]}'",
                ))
    return entities, merge_records
# ---------------------------------------------------------------------------
# 2. Entity Canonicalization
# ---------------------------------------------------------------------------
def canonicalize_entities(
    entities: list[Entity],
) -> tuple[list[Entity], dict[str, str], list[MergeRecord]]:
    """
    Merge entities that refer to the same real-world thing.
    Strategy:
      - People: merge by github_id, then by login (case-insensitive)
      - Labels: merge by name (case-insensitive)
      - Topics: merge by normalized name
      - Milestones: merge by title
      - Repos: merge by name
    Returns:
      - Updated entity list (with canonical_id set on duplicates)
      - ID mapping: old_id -> canonical_id
      - Merge records for audit
    """
    merge_records: list[MergeRecord] = []
    id_map: dict[str, str] = {}  # old_id -> canonical_id
    # Group by entity type
    by_type: dict[EntityType, list[Entity]] = defaultdict(list)
    for e in entities:
        by_type[e.entity_type].append(e)
    # --- People: merge by github_id ---
    people = by_type.get(EntityType.PERSON, [])
    people_by_gid: dict[int, list[Entity]] = defaultdict(list)
    for p in people:
        gid = p.properties.get("github_id")
        if gid is not None:
            people_by_gid[gid].append(p)
    for gid, group in people_by_gid.items():
        if len(group) <= 1:
            continue
        # Pick canonical: earliest first_seen, or first encountered
        canonical = min(group, key=lambda e: (to_utc(e.first_seen) or datetime.max, e.id))
        for p in group:
            if p.id == canonical.id:
                continue
            p.canonical_id = canonical.id
            id_map[p.id] = canonical.id
            # Merge aliases
            for alias in p.aliases:
                if alias not in canonical.aliases:
                    canonical.aliases.append(alias)
            if p.name not in canonical.aliases:
                canonical.aliases.append(p.name)
            # Merge first_seen / last_seen
            if p.first_seen and (not canonical.first_seen or p.first_seen < canonical.first_seen):
                canonical.first_seen = p.first_seen
            if p.last_seen and (not canonical.last_seen or p.last_seen > canonical.last_seen):
                canonical.last_seen = p.last_seen
            merge_records.append(MergeRecord(
                merge_type="entity",
                source_id=p.id,
                target_id=canonical.id,
                reason=f"Same github_id={gid}",
            ))
    # --- Labels, Topics, Milestones: merge by normalized name ---
    for etype in [EntityType.LABEL, EntityType.TOPIC, EntityType.MILESTONE]:
        group = by_type.get(etype, [])
        by_name: dict[str, list[Entity]] = defaultdict(list)
        for e in group:
            key = _normalize_text(e.name)
            by_name[key].append(e)
        for name, ents in by_name.items():
            if len(ents) <= 1:
                continue
            canonical = ents[0]
            for e in ents[1:]:
                e.canonical_id = canonical.id
                id_map[e.id] = canonical.id
                for alias in e.aliases:
                    if alias not in canonical.aliases:
                        canonical.aliases.append(alias)
                merge_records.append(MergeRecord(
                    merge_type="entity",
                    source_id=e.id,
                    target_id=canonical.id,
                    reason=f"Same normalized name: '{name}'",
                ))
    # --- Repos: merge by name ---
    repos = by_type.get(EntityType.REPOSITORY, [])
    repos_by_name: dict[str, list[Entity]] = defaultdict(list)
    for r in repos:
        repos_by_name[r.name.lower()].append(r)
    for name, ents in repos_by_name.items():
        if len(ents) <= 1:
            continue
        canonical = ents[0]
        for e in ents[1:]:
            e.canonical_id = canonical.id
            id_map[e.id] = canonical.id
            merge_records.append(MergeRecord(
                merge_type="entity",
                source_id=e.id,
                target_id=canonical.id,
                reason=f"Same repo name: '{name}'",
            ))
    return entities, id_map, merge_records
# ---------------------------------------------------------------------------
# 3. Claim Deduplication
# ---------------------------------------------------------------------------
def dedup_claims(
    claims: list[Claim],
    id_map: dict[str, str],
) -> tuple[list[Claim], list[MergeRecord]]:
    """
    Deduplicate claims:
      1. Remap entity IDs using the canonicalization map
      2. Merge claims with same (subject, predicate, object) triple
      3. For status changes, mark older claims as superseded
    Returns updated claim list and merge records.
    """
    merge_records: list[MergeRecord] = []
    # Step 1: Remap entity IDs
    for claim in claims:
        if claim.subject_id in id_map:
            claim.subject_id = id_map[claim.subject_id]
        if claim.object_id and claim.object_id in id_map:
            claim.object_id = id_map[claim.object_id]
    # Step 2: Group by (type, subject, predicate, object/value) and merge
    claim_groups: dict[str, list[Claim]] = defaultdict(list)
    for claim in claims:
        # Build dedup key
        obj_key = claim.object_id or claim.object_value or ""
        key = f"{claim.claim_type}|{claim.subject_id}|{claim.predicate}|{obj_key}"
        claim_groups[key].append(claim)
    deduped_claims: list[Claim] = []
    for key, group in claim_groups.items():
        if len(group) == 1:
            deduped_claims.append(group[0])
            continue
        # Keep the one with highest confidence; merge evidence
        canonical = max(group, key=lambda c: (c.confidence, len(c.evidence_ids)))
        all_evidence: list[str] = []
        for c in group:
            for eid in c.evidence_ids:
                if eid not in all_evidence:
                    all_evidence.append(eid)
        canonical.evidence_ids = all_evidence
        # Update confidence based on evidence count (more evidence = higher confidence)
        if len(all_evidence) > 1:
            canonical.confidence = min(1.0, canonical.confidence + 0.05 * (len(all_evidence) - 1))
        deduped_claims.append(canonical)
        for c in group:
            if c.id != canonical.id:
                merge_records.append(MergeRecord(
                    merge_type="claim",
                    source_id=c.id,
                    target_id=canonical.id,
                    reason=f"Duplicate claim: {key[:80]}",
                ))
    # Step 3: Handle status changes — mark older ones as superseded
    _handle_status_supersession(deduped_claims)
    return deduped_claims, merge_records
def _handle_status_supersession(claims: list[Claim]) -> None:
    """For status change claims on the same entity, mark older ones as not current."""
    status_claims: dict[str, list[Claim]] = defaultdict(list)
    for c in claims:
        if c.claim_type == ClaimType.STATUS_CHANGE:
            status_claims[c.subject_id].append(c)
    for subject_id, group in status_claims.items():
        if len(group) <= 1:
            continue
        # Sort by valid_from
        group.sort(key=lambda c: (c.valid_from or datetime.min))
        # Mark all but the latest as not current
        for c in group[:-1]:
            c.is_current = False
            c.superseded_by = group[-1].id
            # Set valid_until based on next event
            idx = group.index(c)
            if idx + 1 < len(group):
                c.valid_until = group[idx + 1].valid_from
# ---------------------------------------------------------------------------
# Full dedup pipeline
# ---------------------------------------------------------------------------
def run_dedup(
    entities: list[Entity],
    claims: list[Claim],
    evidence: list[Evidence],
) -> tuple[list[Entity], list[Claim], list[Evidence], list[MergeRecord]]:
    """
    Run the full deduplication pipeline.
    Returns:
      - Deduplicated entities (canonical only)
      - Deduplicated claims
      - Evidence (unchanged)
      - All merge records
    """
    all_merges: list[MergeRecord] = []
    # 1. Artifact dedup
    entities, artifact_merges = dedup_artifacts(entities, claims)
    all_merges.extend(artifact_merges)
    # 2. Entity canonicalization
    entities, id_map, entity_merges = canonicalize_entities(entities)
    all_merges.extend(entity_merges)
    # 3. Claim dedup (with ID remapping)
    claims, claim_merges = dedup_claims(claims, id_map)
    all_merges.extend(claim_merges)
    # 4. Filter to canonical entities only
    canonical_entities = [e for e in entities if e.canonical_id is None]
    # 5. Deduplicate evidence
    seen_ev: dict[str, Evidence] = {}
    for ev in evidence:
        if ev.id not in seen_ev:
            seen_ev[ev.id] = ev
    evidence = list(seen_ev.values())
    print(f"  Dedup: {len(all_merges)} merges "
          f"({sum(1 for m in all_merges if m.merge_type == 'entity')} entity, "
          f"{sum(1 for m in all_merges if m.merge_type == 'claim')} claim, "
          f"{sum(1 for m in all_merges if m.merge_type == 'artifact')} artifact)")
    return canonical_entities, claims, evidence, all_merges