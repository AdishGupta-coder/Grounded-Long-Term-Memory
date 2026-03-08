"""
Corpus downloader for GitHub Issues.
Fetches issues + comments from a public GitHub repository using the REST API.
No authentication required for public repos (rate-limited to 60 req/hr).
"""
from __future__ import annotations
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any
import requests
DEFAULT_REPO = "facebook/react"
DEFAULT_ISSUE_COUNT = 150
API_BASE = "https://api.github.com"
RATE_LIMIT_PAUSE = 2  # seconds between requests to avoid 403
def _headers() -> dict[str, str]:
    """Return API headers, including token if available."""
    h: dict[str, str] = {"Accept": "application/vnd.github.v3+json"}
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        h["Authorization"] = f"token {token}"
    return h
def _get_json(url: str, params: dict | None = None) -> Any:
    """GET with retry on rate-limit."""
    for attempt in range(3):
        resp = requests.get(url, headers=_headers(), params=params, timeout=30)
        if resp.status_code == 403 and "rate limit" in resp.text.lower():
            wait = int(resp.headers.get("Retry-After", 60))
            print(f"  Rate-limited, waiting {wait}s …")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError(f"Failed after 3 attempts: {url}")
def fetch_issues(
    repo: str = DEFAULT_REPO,
    count: int = DEFAULT_ISSUE_COUNT,
    state: str = "all",
) -> list[dict]:
    """Fetch *count* issues (including PRs) from the repo."""
    issues: list[dict] = []
    page = 1
    per_page = min(count, 100)
    while len(issues) < count:
        url = f"{API_BASE}/repos/{repo}/issues"
        params = {
            "state": state,
            "per_page": per_page,
            "page": page,
            "sort": "comments",  # most-discussed first
            "direction": "desc",
        }
        batch = _get_json(url, params)
        if not batch:
            break
        issues.extend(batch)
        page += 1
        time.sleep(RATE_LIMIT_PAUSE)
    return issues[:count]
def fetch_comments(issue_url: str, max_comments: int = 50) -> list[dict]:
    """Fetch comments for a single issue."""
    comments: list[dict] = []
    page = 1
    while True:
        params = {"per_page": min(max_comments, 100), "page": page}
        batch = _get_json(issue_url, params)
        if not batch:
            break
        comments.extend(batch)
        if len(comments) >= max_comments:
            break
        page += 1
        time.sleep(RATE_LIMIT_PAUSE)
    return comments[:max_comments]
def fetch_events(events_url: str) -> list[dict]:
    """Fetch timeline events (label changes, assignments, state changes)."""
    try:
        return _get_json(events_url, {"per_page": 100})
    except Exception:
        return []
def download_corpus(
    repo: str = DEFAULT_REPO,
    count: int = DEFAULT_ISSUE_COUNT,
    output_dir: str = "outputs",
    max_comments_per_issue: int = 30,
) -> Path:
    """
    Download a corpus of issues + comments + events from a GitHub repo.
    Returns the path to the saved JSON file.
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    corpus_path = out / "corpus.json"
    if corpus_path.exists():
        print(f"Corpus already exists at {corpus_path}, loading …")
        with open(corpus_path) as f:
            data = json.load(f)
        print(f"  Loaded {len(data['issues'])} issues")
        return corpus_path
    print(f"Fetching {count} issues from {repo} …")
    raw_issues = fetch_issues(repo, count)
    print(f"  Got {len(raw_issues)} issues")
    corpus: list[dict] = []
    for i, issue in enumerate(raw_issues):
        print(f"  [{i+1}/{len(raw_issues)}] #{issue['number']} – {issue['title'][:60]}")
        comments = []
        if issue.get("comments", 0) > 0:
            comments = fetch_comments(
                issue["comments_url"], max_comments=max_comments_per_issue
            )
        events = []
        if issue.get("events_url"):
            events = fetch_events(issue["events_url"])
            time.sleep(RATE_LIMIT_PAUSE)
        corpus.append(
            {
                "issue": _slim_issue(issue),
                "comments": [_slim_comment(c) for c in comments],
                "events": [_slim_event(e) for e in events],
            }
        )
    data = {
        "repo": repo,
        "fetched_at": datetime.utcnow().isoformat(),
        "issue_count": len(corpus),
        "issues": corpus,
    }
    with open(corpus_path, "w") as f:
        json.dump(data, f, indent=2, default=str)
    print(f"Corpus saved to {corpus_path}")
    return corpus_path
def _slim_issue(issue: dict) -> dict:
    """Keep only the fields we need from an issue."""
    return {
        "number": issue["number"],
        "title": issue["title"],
        "body": (issue.get("body") or "")[:5000],
        "state": issue["state"],
        "html_url": issue["html_url"],
        "user": {
            "login": issue["user"]["login"],
            "id": issue["user"]["id"],
            "html_url": issue["user"]["html_url"],
        },
        "labels": [
            {"name": l["name"], "description": l.get("description", "")}
            for l in issue.get("labels", [])
        ],
        "assignees": [
            {"login": a["login"], "id": a["id"]}
            for a in issue.get("assignees", [])
        ],
        "milestone": (
            {
                "title": issue["milestone"]["title"],
                "state": issue["milestone"]["state"],
                "number": issue["milestone"]["number"],
            }
            if issue.get("milestone")
            else None
        ),
        "created_at": issue["created_at"],
        "updated_at": issue["updated_at"],
        "closed_at": issue.get("closed_at"),
        "is_pull_request": "pull_request" in issue,
        "comment_count": issue.get("comments", 0),
    }
def _slim_comment(comment: dict) -> dict:
    """Keep only the fields we need from a comment."""
    return {
        "id": comment["id"],
        "body": (comment.get("body") or "")[:3000],
        "user": {
            "login": comment["user"]["login"],
            "id": comment["user"]["id"],
        },
        "created_at": comment["created_at"],
        "updated_at": comment["updated_at"],
        "html_url": comment["html_url"],
    }
def _slim_event(event: dict) -> dict:
    """Keep only the fields we need from an event."""
    return {
        "id": event.get("id"),
        "event": event.get("event"),
        "actor": (
            {"login": event["actor"]["login"], "id": event["actor"]["id"]}
            if event.get("actor")
            else None
        ),
        "created_at": event.get("created_at"),
        "label": event.get("label"),
        "assignee": (
            {"login": event["assignee"]["login"]}
            if event.get("assignee")
            else None
        ),
        "milestone": (
            {"title": event["milestone"]["title"]}
            if event.get("milestone")
            else None
        ),
        "rename": event.get("rename"),
    }
if __name__ == "__main__":
    download_corpus()