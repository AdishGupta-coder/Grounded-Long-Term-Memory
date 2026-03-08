"""
Memory Graph Store — SQLite-backed.
Stores entities, claims, evidence, and merge records.
Supports querying, incremental updates, and serialization.
"""
from __future__ import annotations
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any
from schema import Entity, EntityType, Claim, ClaimType, Evidence, EvidenceSourceType, MergeRecord
# ---------------------------------------------------------------------------
# Database schema
# ---------------------------------------------------------------------------
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS entities (
    id TEXT PRIMARY KEY,
    canonical_id TEXT,
    entity_type TEXT NOT NULL,
    name TEXT NOT NULL,
    aliases TEXT DEFAULT '[]',
    properties TEXT DEFAULT '{}',
    first_seen TEXT,
    last_seen TEXT
);
CREATE TABLE IF NOT EXISTS claims (
    id TEXT PRIMARY KEY,
    claim_type TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    predicate TEXT NOT NULL,
    object_id TEXT,
    object_value TEXT,
    confidence REAL DEFAULT 1.0,
    valid_from TEXT,
    valid_until TEXT,
    is_current INTEGER DEFAULT 1,
    extraction_version TEXT DEFAULT 'v1',
    superseded_by TEXT
);
CREATE TABLE IF NOT EXISTS evidence (
    id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,
    source_type TEXT NOT NULL,
    source_url TEXT DEFAULT '',
    excerpt TEXT NOT NULL,
    offset_start INTEGER,
    offset_end INTEGER,
    timestamp TEXT,
    extraction_version TEXT DEFAULT 'v1'
);
CREATE TABLE IF NOT EXISTS claim_evidence (
    claim_id TEXT NOT NULL,
    evidence_id TEXT NOT NULL,
    PRIMARY KEY (claim_id, evidence_id)
);
CREATE TABLE IF NOT EXISTS merge_log (
    id TEXT PRIMARY KEY,
    merge_type TEXT NOT NULL,
    source_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    reason TEXT DEFAULT '',
    merged_at TEXT,
    reversed_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(entity_type);
CREATE INDEX IF NOT EXISTS idx_entities_name ON entities(name);
CREATE INDEX IF NOT EXISTS idx_claims_subject ON claims(subject_id);
CREATE INDEX IF NOT EXISTS idx_claims_object ON claims(object_id);
CREATE INDEX IF NOT EXISTS idx_claims_type ON claims(claim_type);
CREATE INDEX IF NOT EXISTS idx_claims_current ON claims(is_current);
CREATE INDEX IF NOT EXISTS idx_evidence_source ON evidence(source_id);
"""
class MemoryGraph:
    """SQLite-backed memory graph store."""
    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA_SQL)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
    def close(self) -> None:
        self.conn.close()
    # -------------------------------------------------------------------
    # Insert / Upsert
    # -------------------------------------------------------------------
    def upsert_entity(self, entity: Entity) -> None:
        """Insert or update an entity (idempotent)."""
        self.conn.execute(
            """INSERT INTO entities (id, canonical_id, entity_type, name, aliases, properties, first_seen, last_seen)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
                 canonical_id = COALESCE(excluded.canonical_id, entities.canonical_id),
                 name = excluded.name,
                 aliases = excluded.aliases,
                 properties = excluded.properties,
                 first_seen = MIN(COALESCE(entities.first_seen, excluded.first_seen), COALESCE(excluded.first_seen, entities.first_seen)),
                 last_seen = MAX(COALESCE(entities.last_seen, excluded.last_seen), COALESCE(excluded.last_seen, entities.last_seen))
            """,
            (
                entity.id,
                entity.canonical_id,
                entity.entity_type.value,
                entity.name,
                json.dumps(entity.aliases),
                json.dumps(entity.properties),
                entity.first_seen.isoformat() if entity.first_seen else None,
                entity.last_seen.isoformat() if entity.last_seen else None,
            ),
        )
    def upsert_claim(self, claim: Claim) -> None:
        """Insert or update a claim (idempotent)."""
        self.conn.execute(
            """INSERT INTO claims (id, claim_type, subject_id, predicate, object_id, object_value,
                                   confidence, valid_from, valid_until, is_current, extraction_version, superseded_by)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
                 confidence = MAX(claims.confidence, excluded.confidence),
                 is_current = excluded.is_current,
                 valid_until = COALESCE(excluded.valid_until, claims.valid_until),
                 superseded_by = COALESCE(excluded.superseded_by, claims.superseded_by)
            """,
            (
                claim.id,
                claim.claim_type.value,
                claim.subject_id,
                claim.predicate,
                claim.object_id,
                claim.object_value,
                claim.confidence,
                claim.valid_from.isoformat() if claim.valid_from else None,
                claim.valid_until.isoformat() if claim.valid_until else None,
                1 if claim.is_current else 0,
                claim.extraction_version,
                claim.superseded_by,
            ),
        )
        # Link evidence
        for eid in claim.evidence_ids:
            self.conn.execute(
                "INSERT OR IGNORE INTO claim_evidence (claim_id, evidence_id) VALUES (?, ?)",
                (claim.id, eid),
            )
    def upsert_evidence(self, evidence: Evidence) -> None:
        """Insert or update an evidence record."""
        self.conn.execute(
            """INSERT INTO evidence (id, source_id, source_type, source_url, excerpt, offset_start, offset_end, timestamp, extraction_version)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO NOTHING
            """,
            (
                evidence.id,
                evidence.source_id,
                evidence.source_type.value,
                evidence.source_url,
                evidence.excerpt,
                evidence.offset_start,
                evidence.offset_end,
                evidence.timestamp.isoformat() if evidence.timestamp else None,
                evidence.extraction_version,
            ),
        )
    def record_merge(self, merge: MergeRecord) -> None:
        """Log a merge operation for audit."""
        self.conn.execute(
            """INSERT OR IGNORE INTO merge_log (id, merge_type, source_id, target_id, reason, merged_at, reversed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                merge.id, merge.merge_type, merge.source_id,
                merge.target_id, merge.reason,
                merge.merged_at.isoformat(),
                merge.reversed_at.isoformat() if merge.reversed_at else None,
            ),
        )
    def bulk_insert(
        self,
        entities: list[Entity],
        claims: list[Claim],
        evidence: list[Evidence],
        merges: list[MergeRecord] | None = None,
    ) -> None:
        """Insert all data in a single transaction."""
        with self.conn:
            for ev in evidence:
                self.upsert_evidence(ev)
            for ent in entities:
                self.upsert_entity(ent)
            for claim in claims:
                self.upsert_claim(claim)
            if merges:
                for merge in merges:
                    self.record_merge(merge)
    def commit(self) -> None:
        self.conn.commit()
    # -------------------------------------------------------------------
    # Query
    # -------------------------------------------------------------------
    def get_entity(self, entity_id: str) -> Entity | None:
        row = self.conn.execute("SELECT * FROM entities WHERE id = ?", (entity_id,)).fetchone()
        return self._row_to_entity(row) if row else None
    def get_entities(
        self,
        entity_type: EntityType | None = None,
        name_search: str | None = None,
        limit: int = 100,
    ) -> list[Entity]:
        query = "SELECT * FROM entities WHERE canonical_id IS NULL"
        params: list[Any] = []
        if entity_type:
            query += " AND entity_type = ?"
            params.append(entity_type.value)
        if name_search:
            query += " AND (name LIKE ? OR aliases LIKE ?)"
            params.extend([f"%{name_search}%", f"%{name_search}%"])
        query += " ORDER BY name LIMIT ?"
        params.append(limit)
        rows = self.conn.execute(query, params).fetchall()
        return [self._row_to_entity(r) for r in rows]
    def get_claims_for_entity(
        self,
        entity_id: str,
        current_only: bool = False,
        claim_type: ClaimType | None = None,
    ) -> list[Claim]:
        query = "SELECT * FROM claims WHERE (subject_id = ? OR object_id = ?)"
        params: list[Any] = [entity_id, entity_id]
        if current_only:
            query += " AND is_current = 1"
        if claim_type:
            query += " AND claim_type = ?"
            params.append(claim_type.value)
        query += " ORDER BY valid_from DESC"
        rows = self.conn.execute(query, params).fetchall()
        return [self._row_to_claim(r) for r in rows]
    def get_evidence_for_claim(self, claim_id: str) -> list[Evidence]:
        rows = self.conn.execute(
            """SELECT e.* FROM evidence e
               JOIN claim_evidence ce ON e.id = ce.evidence_id
               WHERE ce.claim_id = ?""",
            (claim_id,),
        ).fetchall()
        return [self._row_to_evidence(r) for r in rows]
    def get_all_claims(self, current_only: bool = False, limit: int = 1000) -> list[Claim]:
        query = "SELECT * FROM claims"
        if current_only:
            query += " WHERE is_current = 1"
        query += " ORDER BY confidence DESC LIMIT ?"
        rows = self.conn.execute(query, (limit,)).fetchall()
        return [self._row_to_claim(r) for r in rows]
    def get_neighbors(self, entity_id: str) -> list[Entity]:
        """Get entities connected to this entity via claims."""
        rows = self.conn.execute(
            """SELECT DISTINCT e.* FROM entities e
               JOIN claims c ON (e.id = c.object_id AND c.subject_id = ?)
                             OR (e.id = c.subject_id AND c.object_id = ?)
               WHERE e.canonical_id IS NULL""",
            (entity_id, entity_id),
        ).fetchall()
        return [self._row_to_entity(r) for r in rows]
    def get_merge_log(self) -> list[MergeRecord]:
        rows = self.conn.execute("SELECT * FROM merge_log ORDER BY merged_at DESC").fetchall()
        return [self._row_to_merge(r) for r in rows]
    def search_entities(self, query: str, limit: int = 20) -> list[Entity]:
        """Full-text search on entity names and aliases."""
        rows = self.conn.execute(
            """SELECT * FROM entities
               WHERE canonical_id IS NULL
                 AND (name LIKE ? OR aliases LIKE ? OR properties LIKE ?)
               ORDER BY name LIMIT ?""",
            (f"%{query}%", f"%{query}%", f"%{query}%", limit),
        ).fetchall()
        return [self._row_to_entity(r) for r in rows]
    def search_claims(self, query: str, limit: int = 20) -> list[Claim]:
        """Search claims by predicate or value."""
        rows = self.conn.execute(
            """SELECT * FROM claims
               WHERE predicate LIKE ? OR object_value LIKE ?
               ORDER BY confidence DESC LIMIT ?""",
            (f"%{query}%", f"%{query}%", limit),
        ).fetchall()
        return [self._row_to_claim(r) for r in rows]
    def stats(self) -> dict[str, int]:
        """Return graph statistics."""
        result = {}
        for table in ["entities", "claims", "evidence", "merge_log"]:
            row = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            result[table] = row[0]
        row = self.conn.execute(
            "SELECT COUNT(*) FROM entities WHERE canonical_id IS NULL"
        ).fetchone()
        result["canonical_entities"] = row[0]
        row = self.conn.execute(
            "SELECT COUNT(*) FROM claims WHERE is_current = 1"
        ).fetchone()
        result["current_claims"] = row[0]
        return result
    def entity_type_counts(self) -> dict[str, int]:
        rows = self.conn.execute(
            "SELECT entity_type, COUNT(*) FROM entities WHERE canonical_id IS NULL GROUP BY entity_type"
        ).fetchall()
        return {r[0]: r[1] for r in rows}
    def claim_type_counts(self) -> dict[str, int]:
        rows = self.conn.execute(
            "SELECT claim_type, COUNT(*) FROM claims GROUP BY claim_type"
        ).fetchall()
        return {r[0]: r[1] for r in rows}
    # -------------------------------------------------------------------
    # Serialization
    # -------------------------------------------------------------------
    def to_json(self) -> dict:
        """Export the entire graph as a JSON-serializable dict."""
        entities = self.get_entities(limit=10000)
        claims = self.get_all_claims(limit=50000)
        evidence_rows = self.conn.execute("SELECT * FROM evidence").fetchall()
        evidence = [self._row_to_evidence(r) for r in evidence_rows]
        merges = self.get_merge_log()
        return {
            "entities": [e.model_dump(mode="json") for e in entities],
            "claims": [c.model_dump(mode="json") for c in claims],
            "evidence": [ev.model_dump(mode="json") for ev in evidence],
            "merge_log": [m.model_dump(mode="json") for m in merges],
            "stats": self.stats(),
        }
    def save_json(self, path: str) -> None:
        """Export graph to a JSON file."""
        data = self.to_json()
        with open(path, "w") as f:
            json.dump(data, f, indent=2, default=str)
        print(f"Graph saved to {path}")
    # -------------------------------------------------------------------
    # Vis.js export (for visualization)
    # -------------------------------------------------------------------
    def to_visjs(self, max_nodes: int = 300, max_edges: int = 500) -> dict:
        """Export graph in vis.js format for visualization."""
        entities = self.get_entities(limit=max_nodes)
        claims = self.get_all_claims(current_only=True, limit=max_edges)
        # Color map by entity type
        colors = {
            "person": "#4CAF50",
            "repository": "#2196F3",
            "issue": "#FF9800",
            "pull_request": "#9C27B0",
            "label": "#F44336",
            "milestone": "#00BCD4",
            "topic": "#FFC107",
        }
        nodes = []
        entity_ids = {e.id for e in entities}
        for e in entities:
            nodes.append({
                "id": e.id,
                "label": e.name[:40],
                "title": f"{e.entity_type.value}: {e.name}\nAliases: {', '.join(e.aliases[:3])}",
                "group": e.entity_type.value,
                "color": colors.get(e.entity_type.value, "#9E9E9E"),
                "shape": "dot" if e.entity_type == EntityType.PERSON else "box",
                "size": 15 if e.entity_type in (EntityType.PERSON, EntityType.REPOSITORY) else 10,
                "entity_type": e.entity_type.value,
                "properties": e.properties,
                "aliases": e.aliases,
                "first_seen": e.first_seen.isoformat() if e.first_seen else None,
                "last_seen": e.last_seen.isoformat() if e.last_seen else None,
            })
        edges = []
        for c in claims:
            if c.object_id and c.subject_id in entity_ids and c.object_id in entity_ids:
                edges.append({
                    "from": c.subject_id,
                    "to": c.object_id,
                    "label": c.predicate[:20],
                    "title": f"{c.predicate}\nConfidence: {c.confidence:.2f}\nCurrent: {c.is_current}",
                    "claim_id": c.id,
                    "claim_type": c.claim_type.value,
                    "confidence": c.confidence,
                    "is_current": c.is_current,
                    "arrows": "to",
                    "dashes": not c.is_current,
                    "color": {"color": "#999" if not c.is_current else "#333"},
                })
        return {"nodes": nodes, "edges": edges}
    # -------------------------------------------------------------------
    # Row converters
    # -------------------------------------------------------------------
    @staticmethod
    def _row_to_entity(row: sqlite3.Row) -> Entity:
        return Entity(
            id=row["id"],
            canonical_id=row["canonical_id"],
            entity_type=EntityType(row["entity_type"]),
            name=row["name"],
            aliases=json.loads(row["aliases"]),
            properties=json.loads(row["properties"]),
            first_seen=datetime.fromisoformat(row["first_seen"]) if row["first_seen"] else None,
            last_seen=datetime.fromisoformat(row["last_seen"]) if row["last_seen"] else None,
        )
    @staticmethod
    def _row_to_claim(row: sqlite3.Row) -> Claim:
        # Get evidence IDs - we'll need to query separately
        return Claim(
            id=row["id"],
            claim_type=ClaimType(row["claim_type"]),
            subject_id=row["subject_id"],
            predicate=row["predicate"],
            object_id=row["object_id"],
            object_value=row["object_value"],
            confidence=row["confidence"],
            valid_from=datetime.fromisoformat(row["valid_from"]) if row["valid_from"] else None,
            valid_until=datetime.fromisoformat(row["valid_until"]) if row["valid_until"] else None,
            is_current=bool(row["is_current"]),
            extraction_version=row["extraction_version"],
            superseded_by=row["superseded_by"],
        )
    @staticmethod
    def _row_to_evidence(row: sqlite3.Row) -> Evidence:
        return Evidence(
            id=row["id"],
            source_id=row["source_id"],
            source_type=EvidenceSourceType(row["source_type"]),
            source_url=row["source_url"],
            excerpt=row["excerpt"],
            offset_start=row["offset_start"],
            offset_end=row["offset_end"],
            timestamp=datetime.fromisoformat(row["timestamp"]) if row["timestamp"] else None,
            extraction_version=row["extraction_version"],
        )
    @staticmethod
    def _row_to_merge(row: sqlite3.Row) -> MergeRecord:
        return MergeRecord(
            id=row["id"],
            merge_type=row["merge_type"],
            source_id=row["source_id"],
            target_id=row["target_id"],
            reason=row["reason"],
            merged_at=datetime.fromisoformat(row["merged_at"]),
            reversed_at=datetime.fromisoformat(row["reversed_at"]) if row["reversed_at"] else None,
        )