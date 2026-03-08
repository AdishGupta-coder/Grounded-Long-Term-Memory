"""
Schema/Ontology definitions for the Layer10 Memory Graph.
Entity types, claim types, evidence structures, and core data models.
Designed for GitHub Issues corpus but extensible to email/chat/tickets.
"""
from __future__ import annotations
import uuid
from datetime import datetime
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field
# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------
class EntityType(str, Enum):
    PERSON = "person"
    REPOSITORY = "repository"
    ISSUE = "issue"
    PULL_REQUEST = "pull_request"
    LABEL = "label"
    MILESTONE = "milestone"
    TOPIC = "topic"  # extracted from unstructured text
class ClaimType(str, Enum):
    AUTHORSHIP = "authorship"          # person authored artifact
    ASSIGNMENT = "assignment"          # person assigned to artifact
    LABELING = "labeling"              # artifact labeled with label
    STATUS_CHANGE = "status_change"    # artifact changed state
    REFERENCE = "reference"            # artifact references another
    MILESTONE_MEMBERSHIP = "milestone" # artifact belongs to milestone
    TOPIC_DISCUSSION = "topic_discussion"  # artifact discusses topic
    DECISION = "decision"              # a decision was made
    TECHNICAL_CLAIM = "technical_claim"  # a technical assertion
class EvidenceSourceType(str, Enum):
    ISSUE_BODY = "issue_body"
    ISSUE_COMMENT = "issue_comment"
    ISSUE_EVENT = "issue_event"
    PR_BODY = "pr_body"
    PR_COMMENT = "pr_comment"
# ---------------------------------------------------------------------------
# Core Models
# ---------------------------------------------------------------------------
def _new_id() -> str:
    return str(uuid.uuid4())
class Evidence(BaseModel):
    """A pointer to a specific piece of source material supporting a claim."""
    id: str = Field(default_factory=_new_id)
    source_id: str                    # unique ID of the source document
    source_type: EvidenceSourceType
    source_url: str = ""              # link to original
    excerpt: str                      # verbatim text excerpt
    offset_start: Optional[int] = None
    offset_end: Optional[int] = None
    timestamp: Optional[datetime] = None
    extraction_version: str = "v1"
class Entity(BaseModel):
    """A node in the memory graph (person, issue, topic, etc.)."""
    id: str = Field(default_factory=_new_id)
    canonical_id: Optional[str] = None   # after dedup, points to canonical
    entity_type: EntityType
    name: str
    aliases: list[str] = Field(default_factory=list)
    properties: dict = Field(default_factory=dict)
    first_seen: Optional[datetime] = None
    last_seen: Optional[datetime] = None
class Claim(BaseModel):
    """An edge / assertion in the memory graph, always grounded in evidence."""
    id: str = Field(default_factory=_new_id)
    claim_type: ClaimType
    subject_id: str                      # entity ID
    predicate: str                       # human-readable relationship
    object_id: Optional[str] = None      # entity ID (for relationships)
    object_value: Optional[str] = None   # literal value (for attributes)
    confidence: float = 1.0
    valid_from: Optional[datetime] = None
    valid_until: Optional[datetime] = None
    is_current: bool = True
    evidence_ids: list[str] = Field(default_factory=list)
    extraction_version: str = "v1"
    superseded_by: Optional[str] = None  # claim ID that replaced this
class MergeRecord(BaseModel):
    """Audit log entry for a merge/dedup operation (reversible)."""
    id: str = Field(default_factory=_new_id)
    merge_type: str           # "entity" or "claim"
    source_id: str            # ID that was merged away
    target_id: str            # ID that survived
    reason: str = ""
    merged_at: datetime = Field(default_factory=datetime.utcnow)
    reversed_at: Optional[datetime] = None
class ContextPack(BaseModel):
    """A grounded answer bundle returned by the retrieval layer."""
    question: str
    entities: list[Entity] = Field(default_factory=list)
    claims: list[Claim] = Field(default_factory=list)
    evidence: list[Evidence] = Field(default_factory=list)
    summary: Optional[str] = None