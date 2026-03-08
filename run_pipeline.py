#!/usr/bin/env python3
"""
End-to-end pipeline: corpus download → extraction → dedup → graph → retrieval examples.
Usage:
    python run_pipeline.py [--repo OWNER/REPO] [--count N] [--no-llm] [--serve]
"""
from __future__ import annotations
import argparse
import json
import sys
import time
from pathlib import Path
# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))
from corpus import download_corpus
from extraction import extract_issue
from dedup import run_dedup
from graph import MemoryGraph
from retrieval import MemoryRetriever
from schema import Entity, Claim, Evidence
OUTPUT_DIR = Path(__file__).parent / "outputs"
def run_pipeline(
    repo: str = "facebook/react",
    issue_count: int = 150,
    use_llm: bool = True,
    serve: bool = False,
) -> None:
    """Run the full pipeline."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    # -----------------------------------------------------------------
    # Step 1: Download corpus
    # -----------------------------------------------------------------
    print("\n" + "=" * 60)
    print("STEP 1: Downloading corpus")
    print("=" * 60)
    corpus_path = download_corpus(
        repo=repo,
        count=issue_count,
        output_dir=str(OUTPUT_DIR),
        max_comments_per_issue=20,
    )
    with open(corpus_path) as f:
        corpus = json.load(f)
    print(f"  Corpus: {corpus['issue_count']} issues from {corpus['repo']}")
    # -----------------------------------------------------------------
    # Step 2: Structured + LLM extraction
    # -----------------------------------------------------------------
    print("\n" + "=" * 60)
    print("STEP 2: Running extraction")
    print("=" * 60)
    all_entities: list[Entity] = []
    all_claims: list[Claim] = []
    all_evidence: list[Evidence] = []
    for i, issue_data in enumerate(corpus["issues"]):
        issue_num = issue_data["issue"]["number"]
        title = issue_data["issue"]["title"][:50]
        print(f"  [{i+1}/{corpus['issue_count']}] #{issue_num}: {title}")
        try:
            entities, claims, evidence = extract_issue(issue_data, use_llm=use_llm)
            all_entities.extend(entities)
            all_claims.extend(claims)
            all_evidence.extend(evidence)
        except Exception as e:
            print(f"    Error extracting #{issue_num}: {e}")
            continue
    print(f"\n  Raw extraction: {len(all_entities)} entities, "
          f"{len(all_claims)} claims, {len(all_evidence)} evidence")
    # -----------------------------------------------------------------
    # Step 3: Deduplication
    # -----------------------------------------------------------------
    print("\n" + "=" * 60)
    print("STEP 3: Running deduplication")
    print("=" * 60)
    entities, claims, evidence, merges = run_dedup(all_entities, all_claims, all_evidence)
    print(f"  After dedup: {len(entities)} entities, "
          f"{len(claims)} claims, {len(evidence)} evidence, "
          f"{len(merges)} merges")
    # -----------------------------------------------------------------
    # Step 4: Build memory graph
    # -----------------------------------------------------------------
    print("\n" + "=" * 60)
    print("STEP 4: Building memory graph")
    print("=" * 60)
    db_path = str(OUTPUT_DIR / "memory.db")
    graph = MemoryGraph(db_path)
    graph.bulk_insert(entities, claims, evidence, merges)
    graph.commit()
    stats = graph.stats()
    print(f"  Graph stats: {json.dumps(stats, indent=2)}")
    print(f"  Entity types: {json.dumps(graph.entity_type_counts(), indent=2)}")
    print(f"  Claim types: {json.dumps(graph.claim_type_counts(), indent=2)}")
    # Save serialized graph
    graph.save_json(str(OUTPUT_DIR / "graph.json"))
    # -----------------------------------------------------------------
    # Step 5: Example retrievals
    # -----------------------------------------------------------------
    print("\n" + "=" * 60)
    print("STEP 5: Running example retrievals")
    print("=" * 60)
    retriever = MemoryRetriever(graph)
    retriever.build_index()
    example_questions = [
        "What are the most common performance issues?",
        "Who are the most active contributors?",
        "What decisions were made about hooks?",
        "What bug reports are related to rendering?",
        "What components have had the most discussion?",
    ]
    context_packs = []
    for question in example_questions:
        print(f"\n  Q: {question}")
        pack = retriever.search(question, top_k=5)
        formatted = MemoryRetriever.format_context_pack(pack)
        print(formatted[:500])
        context_packs.append(pack.model_dump(mode="json"))
    # Save example context packs
    examples_path = OUTPUT_DIR / "example_context_packs.json"
    with open(examples_path, "w") as f:
        json.dump(
            {"questions": example_questions, "context_packs": context_packs},
            f,
            indent=2,
            default=str,
        )
    print(f"\n  Example context packs saved to {examples_path}")
    # -----------------------------------------------------------------
    # Step 6: Serve (optional)
    # -----------------------------------------------------------------
    if serve:
        print("\n" + "=" * 60)
        print("STEP 6: Starting visualization server")
        print("=" * 60)
        print("  Open http://localhost:8000 in your browser")
        from server import run_server
        run_server()
    print("\n" + "=" * 60)
    print("Pipeline complete!")
    print(f"  Outputs: {OUTPUT_DIR}")
    print(f"  Database: {db_path}")
    print(f"  Graph JSON: {OUTPUT_DIR / 'graph.json'}")
    print(f"  Examples: {examples_path}")
    print("=" * 60)
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Layer10 Memory Graph Pipeline")
    parser.add_argument("--repo", default="facebook/react", help="GitHub repo (owner/repo)")
    parser.add_argument("--count", type=int, default=150, help="Number of issues to fetch")
    parser.add_argument("--no-llm", action="store_true", help="Skip LLM extraction (keyword fallback)")
    parser.add_argument("--serve", action="store_true", help="Start visualization server after pipeline")
    args = parser.parse_args()
    run_pipeline(
        repo=args.repo,
        issue_count=args.count,
        use_llm=not args.no_llm,
        serve=args.serve,
    )