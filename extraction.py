"""
Structured extraction pipeline.
Hybrid approach:
  1. Rule-based extraction for structured GitHub fields (author, labels, state, etc.)
  2. LLM-based extraction (via Ollama) for unstructured content (topics, decisions, claims)
Every extracted claim is grounded in evidence with source pointers.
"""
from __future__ import annotations
import json
import re
import hashlib
from datetime import datetime
from typing import Any
import requests
from schema import (
    Entity, EntityType, Claim, ClaimType, Evidence, EvidenceSourceType,
)
OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL_NAME = "qwen2.5:3b"
# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _parse_dt(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
def _stable_id(*parts: str) -> str:
    """Deterministic ID from string parts."""
    raw = "|".join(str(p) for p in parts)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]
def _extract_issue_refs(text: str) -> list[int]:
    """Find #NNN references in text."""
    return [int(m) for m in re.findall(r'#(\d+)', text)]
# ---------------------------------------------------------------------------
# LLM helpers
# ---------------------------------------------------------------------------
def _call_ollama(prompt: str, max_tokens: int = 1024) -> str:
    """Call Ollama for generation. Returns raw text."""
    try:
        resp = requests.post(
            OLLAMA_URL,
            json={
                "model": MODEL_NAME,
                "prompt": prompt,
                "stream": False,
                "options": {"num_predict": max_tokens, "temperature": 0.1},
            },
            timeout=120,
        )
        resp.raise_for_status()
        return resp.json().get("response", "")
    except Exception as e:
        print(f"  [LLM] Ollama call failed: {e}")
        return ""
def _parse_json_from_llm(text: str) -> list[dict] | None:
    """Try to parse JSON array from LLM output, with repair."""
    text = text.strip()
    # Find JSON array in the response
    start = text.find("[")
    end = text.rfind("]")
    if start == -1 or end == -1:
        # Try single object
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1:
            return None
        try:
            obj = json.loads(text[start : end + 1])
            return [obj]
        except json.JSONDecodeError:
            return None
    try:
        return json.loads(text[start : end + 1])
    except json.JSONDecodeError:
        # Attempt repair: remove trailing commas
        cleaned = re.sub(r',\s*([}\]])', r'\1', text[start : end + 1])
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            return None
# ---------------------------------------------------------------------------
# Rule-based extraction (structured fields)
# ---------------------------------------------------------------------------
def extract_structured(issue_data: dict) -> tuple[list[Entity], list[Claim], list[Evidence]]:
    """
    Extract entities, claims, and evidence from structured GitHub issue fields.
    No LLM needed — this is deterministic.
    """
    entities: list[Entity] = []
    claims: list[Claim] = []
    evidence_list: list[Evidence] = []
    issue = issue_data["issue"]
    comments = issue_data.get("comments", [])
    events = issue_data.get("events", [])
    repo_parts = issue["html_url"].split("/")
    repo_name = f"{repo_parts[3]}/{repo_parts[4]}" if len(repo_parts) > 4 else "unknown/repo"
    created_at = _parse_dt(issue["created_at"])
    issue_number = issue["number"]
    is_pr = issue.get("is_pull_request", False)
    etype = EntityType.PULL_REQUEST if is_pr else EntityType.ISSUE
    # --- Repo entity ---
    repo_id = _stable_id("repo", repo_name)
    entities.append(Entity(
        id=repo_id,
        entity_type=EntityType.REPOSITORY,
        name=repo_name,
        properties={"html_url": f"https://github.com/{repo_name}"},
        first_seen=created_at,
    ))
    # --- Issue / PR entity ---
    issue_id = _stable_id("issue", repo_name, str(issue_number))
    entities.append(Entity(
        id=issue_id,
        entity_type=etype,
        name=f"#{issue_number}: {issue['title']}",
        properties={
            "number": issue_number,
            "state": issue["state"],
            "html_url": issue["html_url"],
            "title": issue["title"],
            "comment_count": issue.get("comment_count", 0),
        },
        first_seen=created_at,
        last_seen=_parse_dt(issue.get("updated_at")),
    ))
    # --- Author entity + authorship claim ---
    user = issue["user"]
    author_id = _stable_id("person", str(user["id"]))
    entities.append(Entity(
        id=author_id,
        entity_type=EntityType.PERSON,
        name=user["login"],
        aliases=[user["login"]],
        properties={"github_id": user["id"], "html_url": user.get("html_url", "")},
        first_seen=created_at,
    ))
    body_evidence = Evidence(
        id=_stable_id("ev", "body", repo_name, str(issue_number)),
        source_id=f"{repo_name}#issue-{issue_number}",
        source_type=EvidenceSourceType.ISSUE_BODY,
        source_url=issue["html_url"],
        excerpt=issue.get("body", "")[:500] or "(no body)",
        timestamp=created_at,
    )
    evidence_list.append(body_evidence)
    claims.append(Claim(
        id=_stable_id("claim", "authored", author_id, issue_id),
        claim_type=ClaimType.AUTHORSHIP,
        subject_id=author_id,
        predicate="authored",
        object_id=issue_id,
        confidence=1.0,
        valid_from=created_at,
        is_current=True,
        evidence_ids=[body_evidence.id],
    ))
    # --- Labels ---
    for label in issue.get("labels", []):
        label_id = _stable_id("label", repo_name, label["name"])
        entities.append(Entity(
            id=label_id,
            entity_type=EntityType.LABEL,
            name=label["name"],
            properties={"description": label.get("description", "")},
        ))
        claims.append(Claim(
            id=_stable_id("claim", "labeled", issue_id, label_id),
            claim_type=ClaimType.LABELING,
            subject_id=issue_id,
            predicate="labeled_with",
            object_id=label_id,
            confidence=1.0,
            valid_from=created_at,
            is_current=True,
            evidence_ids=[body_evidence.id],
        ))
    # --- Assignees ---
    for assignee in issue.get("assignees", []):
        a_id = _stable_id("person", str(assignee["id"]))
        entities.append(Entity(
            id=a_id,
            entity_type=EntityType.PERSON,
            name=assignee["login"],
            aliases=[assignee["login"]],
            properties={"github_id": assignee["id"]},
        ))
        claims.append(Claim(
            id=_stable_id("claim", "assigned", issue_id, a_id),
            claim_type=ClaimType.ASSIGNMENT,
            subject_id=issue_id,
            predicate="assigned_to",
            object_id=a_id,
            confidence=1.0,
            valid_from=created_at,
            is_current=True,
            evidence_ids=[body_evidence.id],
        ))
    # --- Milestone ---
    if issue.get("milestone"):
        ms = issue["milestone"]
        ms_id = _stable_id("milestone", repo_name, str(ms.get("number", ms["title"])))
        entities.append(Entity(
            id=ms_id,
            entity_type=EntityType.MILESTONE,
            name=ms["title"],
            properties={"state": ms.get("state", "open")},
        ))
        claims.append(Claim(
            id=_stable_id("claim", "milestone", issue_id, ms_id),
            claim_type=ClaimType.MILESTONE_MEMBERSHIP,
            subject_id=issue_id,
            predicate="part_of_milestone",
            object_id=ms_id,
            confidence=1.0,
            valid_from=created_at,
            is_current=True,
            evidence_ids=[body_evidence.id],
        ))
    # --- Cross-references from body ---
    refs = _extract_issue_refs(issue.get("body", "") or "")
    for ref_num in refs:
        ref_id = _stable_id("issue", repo_name, str(ref_num))
        claims.append(Claim(
            id=_stable_id("claim", "ref", issue_id, ref_id),
            claim_type=ClaimType.REFERENCE,
            subject_id=issue_id,
            predicate="references",
            object_id=ref_id,
            confidence=0.9,
            valid_from=created_at,
            is_current=True,
            evidence_ids=[body_evidence.id],
        ))
    # --- Comment authors ---
    for comment in comments:
        c_user = comment["user"]
        c_author_id = _stable_id("person", str(c_user["id"]))
        entities.append(Entity(
            id=c_author_id,
            entity_type=EntityType.PERSON,
            name=c_user["login"],
            aliases=[c_user["login"]],
            properties={"github_id": c_user["id"]},
            first_seen=_parse_dt(comment["created_at"]),
        ))
        c_evidence = Evidence(
            id=_stable_id("ev", "comment", str(comment["id"])),
            source_id=f"{repo_name}#comment-{comment['id']}",
            source_type=EvidenceSourceType.ISSUE_COMMENT,
            source_url=comment.get("html_url", ""),
            excerpt=comment.get("body", "")[:500],
            timestamp=_parse_dt(comment["created_at"]),
        )
        evidence_list.append(c_evidence)
        # Cross-references from comments
        c_refs = _extract_issue_refs(comment.get("body", "") or "")
        for ref_num in c_refs:
            ref_id = _stable_id("issue", repo_name, str(ref_num))
            claims.append(Claim(
                id=_stable_id("claim", "ref", issue_id, ref_id, str(comment["id"])),
                claim_type=ClaimType.REFERENCE,
                subject_id=issue_id,
                predicate="references",
                object_id=ref_id,
                confidence=0.8,
                valid_from=_parse_dt(comment["created_at"]),
                is_current=True,
                evidence_ids=[c_evidence.id],
            ))
    # --- Events (state changes, label changes, assignments) ---
    for event in events:
        ev_type = event.get("event")
        actor = event.get("actor")
        ev_time = _parse_dt(event.get("created_at"))
        if not actor:
            continue
        actor_id = _stable_id("person", str(actor["id"]))
        ev = Evidence(
            id=_stable_id("ev", "event", str(event.get("id", "")), str(issue_number)),
            source_id=f"{repo_name}#event-{event.get('id', 'unknown')}",
            source_type=EvidenceSourceType.ISSUE_EVENT,
            source_url=issue["html_url"],
            excerpt=f"Event: {ev_type} by {actor['login']}",
            timestamp=ev_time,
        )
        evidence_list.append(ev)
        if ev_type == "closed":
            claims.append(Claim(
                id=_stable_id("claim", "status", issue_id, "closed", str(event.get("id", ""))),
                claim_type=ClaimType.STATUS_CHANGE,
                subject_id=issue_id,
                predicate="closed_by",
                object_id=actor_id,
                object_value="closed",
                confidence=1.0,
                valid_from=ev_time,
                is_current=True,
                evidence_ids=[ev.id],
            ))
        elif ev_type == "reopened":
            claims.append(Claim(
                id=_stable_id("claim", "status", issue_id, "reopened", str(event.get("id", ""))),
                claim_type=ClaimType.STATUS_CHANGE,
                subject_id=issue_id,
                predicate="reopened_by",
                object_id=actor_id,
                object_value="reopened",
                confidence=1.0,
                valid_from=ev_time,
                is_current=True,
                evidence_ids=[ev.id],
            ))
        elif ev_type == "labeled" and event.get("label"):
            label_name = event["label"]["name"]
            label_id = _stable_id("label", repo_name, label_name)
            claims.append(Claim(
                id=_stable_id("claim", "labeled_event", issue_id, label_id, str(event.get("id", ""))),
                claim_type=ClaimType.LABELING,
                subject_id=issue_id,
                predicate="labeled_with",
                object_id=label_id,
                confidence=1.0,
                valid_from=ev_time,
                is_current=True,
                evidence_ids=[ev.id],
            ))
    return entities, claims, evidence_list
# ---------------------------------------------------------------------------
# LLM-based extraction (unstructured content)
# ---------------------------------------------------------------------------
TOPIC_PROMPT = """You are an information extraction system. Given a GitHub issue title and body, extract the main technical topics discussed.
Return a JSON array of objects with these fields:
- "topic": a short name for the topic (2-5 words, lowercase)
- "confidence": a float 0.0-1.0
Only return the JSON array, nothing else. Extract 1-5 topics.
Title: {title}
Body: {body}
"""
DECISION_PROMPT = """You are an information extraction system. Given a GitHub issue discussion (title + comments), identify any decisions or conclusions reached.
Return a JSON array of objects with these fields:
- "decision": a one-sentence summary of the decision
- "confidence": a float 0.0-1.0
- "comment_index": which comment (0-based) contains the decision, or -1 if in the issue body
Only return the JSON array, nothing else. If no decisions found, return [].
Title: {title}
Body: {body}
Comments:
{comments}
"""
CLAIM_PROMPT = """You are an information extraction system. Given a GitHub issue title and body, extract key technical claims or assertions made.
Return a JSON array of objects with these fields:
- "claim": a one-sentence factual assertion
- "confidence": a float 0.0-1.0
Only return the JSON array, nothing else. Extract 0-3 claims.
Title: {title}
Body: {body}
"""
def extract_unstructured(
    issue_data: dict,
    use_llm: bool = True,
) -> tuple[list[Entity], list[Claim], list[Evidence]]:
    """
    Extract topics, decisions, and technical claims from unstructured text.
    Uses Ollama if available, otherwise falls back to keyword extraction.
    """
    entities: list[Entity] = []
    claims: list[Claim] = []
    evidence_list: list[Evidence] = []
    issue = issue_data["issue"]
    comments = issue_data.get("comments", [])
    repo_parts = issue["html_url"].split("/")
    repo_name = f"{repo_parts[3]}/{repo_parts[4]}" if len(repo_parts) > 4 else "unknown/repo"
    issue_number = issue["number"]
    created_at = _parse_dt(issue["created_at"])
    issue_id = _stable_id("issue", repo_name, str(issue_number))
    body = issue.get("body", "") or ""
    title = issue.get("title", "")
    body_evidence = Evidence(
        id=_stable_id("ev", "body", repo_name, str(issue_number)),
        source_id=f"{repo_name}#issue-{issue_number}",
        source_type=EvidenceSourceType.ISSUE_BODY,
        source_url=issue["html_url"],
        excerpt=body[:500] or "(no body)",
        timestamp=created_at,
    )
    if use_llm:
        # --- Topic extraction ---
        topics = _extract_topics_llm(title, body, issue_id, repo_name, issue_number, created_at, body_evidence)
        for t_ent, t_claim in topics:
            entities.append(t_ent)
            claims.append(t_claim)
        # --- Decision extraction ---
        if comments:
            decisions = _extract_decisions_llm(
                title, body, comments, issue_id, repo_name, issue_number, created_at, body_evidence, evidence_list
            )
            claims.extend(decisions)
        # --- Technical claims ---
        tech_claims = _extract_claims_llm(title, body, issue_id, created_at, body_evidence)
        claims.extend(tech_claims)
    else:
        # Fallback: keyword-based topic extraction
        topics = _extract_topics_keyword(title, body, issue_id, repo_name, issue_number, created_at, body_evidence)
        for t_ent, t_claim in topics:
            entities.append(t_ent)
            claims.append(t_claim)
    return entities, claims, evidence_list
def _extract_topics_llm(
    title: str, body: str, issue_id: str, repo_name: str,
    issue_number: int, created_at: datetime | None, body_evidence: Evidence,
) -> list[tuple[Entity, Claim]]:
    """Extract topics using LLM."""
    prompt = TOPIC_PROMPT.format(title=title, body=body[:2000])
    raw = _call_ollama(prompt, max_tokens=512)
    parsed = _parse_json_from_llm(raw)
    results = []
    if parsed:
        for item in parsed[:5]:
            topic_name = str(item.get("topic", "")).strip().lower()
            if not topic_name or len(topic_name) < 2:
                continue
            confidence = float(item.get("confidence", 0.7))
            if confidence < 0.3:
                continue
            topic_id = _stable_id("topic", topic_name)
            ent = Entity(
                id=topic_id,
                entity_type=EntityType.TOPIC,
                name=topic_name,
                first_seen=created_at,
            )
            claim = Claim(
                id=_stable_id("claim", "topic", issue_id, topic_id),
                claim_type=ClaimType.TOPIC_DISCUSSION,
                subject_id=issue_id,
                predicate="discusses_topic",
                object_id=topic_id,
                confidence=confidence,
                valid_from=created_at,
                is_current=True,
                evidence_ids=[body_evidence.id],
            )
            results.append((ent, claim))
    return results
def _extract_decisions_llm(
    title: str, body: str, comments: list[dict], issue_id: str,
    repo_name: str, issue_number: int, created_at: datetime | None,
    body_evidence: Evidence, evidence_list: list[Evidence],
) -> list[Claim]:
    """Extract decisions from comments using LLM."""
    comment_texts = "\n".join(
        f"[{i}] @{c['user']['login']}: {c.get('body', '')[:300]}"
        for i, c in enumerate(comments[:10])
    )
    prompt = DECISION_PROMPT.format(
        title=title, body=body[:1000], comments=comment_texts
    )
    raw = _call_ollama(prompt, max_tokens=512)
    parsed = _parse_json_from_llm(raw)
    results = []
    if parsed:
        for item in parsed[:3]:
            decision = str(item.get("decision", "")).strip()
            if not decision or len(decision) < 5:
                continue
            confidence = float(item.get("confidence", 0.6))
            comment_idx = int(item.get("comment_index", -1))
            if 0 <= comment_idx < len(comments):
                c = comments[comment_idx]
                ev = Evidence(
                    id=_stable_id("ev", "decision", str(c["id"])),
                    source_id=f"{repo_name}#comment-{c['id']}",
                    source_type=EvidenceSourceType.ISSUE_COMMENT,
                    source_url=c.get("html_url", ""),
                    excerpt=c.get("body", "")[:500],
                    timestamp=_parse_dt(c["created_at"]),
                )
                evidence_list.append(ev)
                ev_ids = [ev.id]
            else:
                ev_ids = [body_evidence.id]
            results.append(Claim(
                id=_stable_id("claim", "decision", issue_id, decision[:50]),
                claim_type=ClaimType.DECISION,
                subject_id=issue_id,
                predicate="decision_made",
                object_value=decision,
                confidence=confidence,
                valid_from=created_at,
                is_current=True,
                evidence_ids=ev_ids,
            ))
    return results
def _extract_claims_llm(
    title: str, body: str, issue_id: str,
    created_at: datetime | None, body_evidence: Evidence,
) -> list[Claim]:
    """Extract technical claims using LLM."""
    if len(body) < 50:
        return []
    prompt = CLAIM_PROMPT.format(title=title, body=body[:2000])
    raw = _call_ollama(prompt, max_tokens=512)
    parsed = _parse_json_from_llm(raw)
    results = []
    if parsed:
        for item in parsed[:3]:
            claim_text = str(item.get("claim", "")).strip()
            if not claim_text or len(claim_text) < 10:
                continue
            confidence = float(item.get("confidence", 0.6))
            results.append(Claim(
                id=_stable_id("claim", "tech", issue_id, claim_text[:50]),
                claim_type=ClaimType.TECHNICAL_CLAIM,
                subject_id=issue_id,
                predicate="asserts",
                object_value=claim_text,
                confidence=confidence,
                valid_from=created_at,
                is_current=True,
                evidence_ids=[body_evidence.id],
            ))
    return results
# ---------------------------------------------------------------------------
# Keyword fallback
# ---------------------------------------------------------------------------
# Common technical terms for fallback extraction
TECH_KEYWORDS = {
    "performance", "memory", "leak", "crash", "bug", "regression",
    "render", "hook", "state", "effect", "component", "context",
    "concurrent", "suspense", "ssr", "hydration", "server",
    "typescript", "type", "api", "breaking", "deprecation",
    "test", "ci", "build", "webpack", "bundler", "tree-shaking",
    "accessibility", "a11y", "animation", "css", "style",
    "event", "handler", "ref", "forward", "lazy", "memo",
    "fiber", "reconciler", "scheduler", "priority", "batch",
    "error", "boundary", "fallback", "loading", "transition",
    "devtools", "profiler", "strict", "mode", "warning",
}
def _extract_topics_keyword(
    title: str, body: str, issue_id: str, repo_name: str,
    issue_number: int, created_at: datetime | None, body_evidence: Evidence,
) -> list[tuple[Entity, Claim]]:
    """Fallback topic extraction using keyword matching."""
    text = f"{title} {body}".lower()
    words = set(re.findall(r'\b\w+\b', text))
    matched = words & TECH_KEYWORDS
    results = []
    for kw in list(matched)[:5]:
        topic_id = _stable_id("topic", kw)
        ent = Entity(
            id=topic_id,
            entity_type=EntityType.TOPIC,
            name=kw,
            first_seen=created_at,
        )
        claim = Claim(
            id=_stable_id("claim", "topic", issue_id, topic_id),
            claim_type=ClaimType.TOPIC_DISCUSSION,
            subject_id=issue_id,
            predicate="discusses_topic",
            object_id=topic_id,
            confidence=0.6,
            valid_from=created_at,
            is_current=True,
            evidence_ids=[body_evidence.id],
        )
        results.append((ent, claim))
    return results
# ---------------------------------------------------------------------------
# Full extraction entry point
# ---------------------------------------------------------------------------
def extract_issue(issue_data: dict, use_llm: bool = True) -> tuple[list[Entity], list[Claim], list[Evidence]]:
    """
    Full extraction for a single issue: structured + unstructured.
    Returns deduplicated entities, claims, and evidence.
    """
    s_entities, s_claims, s_evidence = extract_structured(issue_data)
    u_entities, u_claims, u_evidence = extract_unstructured(issue_data, use_llm=use_llm)
    # Merge (dedup happens later in the pipeline)
    all_entities = s_entities + u_entities
    all_claims = s_claims + u_claims
    all_evidence = s_evidence + u_evidence
    return all_entities, all_claims, all_evidence