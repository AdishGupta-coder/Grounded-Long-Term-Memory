"""
FastAPI server for the Memory Graph visualization and retrieval API.
Serves:
  - Static web UI files
  - REST API for graph exploration and search
"""
from __future__ import annotations
import os
import sys
import json
from pathlib import Path
from typing import Optional
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from graph import MemoryGraph
from retrieval import MemoryRetriever
from schema import EntityType, ClaimType
WEB_DIR = Path(__file__).parent.parent / "web"
DB_PATH = Path(__file__).parent.parent / "outputs" / "memory.db"
app = FastAPI(title="Layer10 Memory Graph", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
# Global state
_graph: MemoryGraph | None = None
_retriever: MemoryRetriever | None = None
def get_graph() -> MemoryGraph:
    global _graph
    if _graph is None:
        db = str(DB_PATH) if DB_PATH.exists() else ":memory:"
        _graph = MemoryGraph(db)
    return _graph
def get_retriever() -> MemoryRetriever:
    global _retriever
    if _retriever is None:
        _retriever = MemoryRetriever(get_graph())
        _retriever.build_index()
    return _retriever
# -------------------------------------------------------------------
# Web UI
# -------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def index():
    index_path = WEB_DIR / "index.html"
    if index_path.exists():
        return FileResponse(index_path)
    return HTMLResponse("<h1>Memory Graph</h1><p>Web UI not found. Run the pipeline first.</p>")
@app.get("/app.js")
async def app_js():
    return FileResponse(WEB_DIR / "app.js", media_type="application/javascript")
@app.get("/styles.css")
async def styles_css():
    return FileResponse(WEB_DIR / "styles.css", media_type="text/css")
# -------------------------------------------------------------------
# API: Graph data
# -------------------------------------------------------------------
@app.get("/api/stats")
async def api_stats():
    graph = get_graph()
    return {
        "stats": graph.stats(),
        "entity_types": graph.entity_type_counts(),
        "claim_types": graph.claim_type_counts(),
    }
@app.get("/api/graph")
async def api_graph(
    max_nodes: int = Query(default=200, le=500),
    max_edges: int = Query(default=400, le=1000),
):
    """Return graph data in vis.js format."""
    graph = get_graph()
    return graph.to_visjs(max_nodes=max_nodes, max_edges=max_edges)
@app.get("/api/entities")
async def api_entities(
    entity_type: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = Query(default=50, le=200),
):
    graph = get_graph()
    etype = EntityType(entity_type) if entity_type else None
    if search:
        entities = graph.search_entities(search, limit=limit)
    else:
        entities = graph.get_entities(entity_type=etype, limit=limit)
    return [e.model_dump(mode="json") for e in entities]
@app.get("/api/entities/{entity_id}")
async def api_entity_detail(entity_id: str):
    graph = get_graph()
    entity = graph.get_entity(entity_id)
    if not entity:
        return JSONResponse({"error": "Not found"}, status_code=404)
    claims = graph.get_claims_for_entity(entity_id)
    neighbors = graph.get_neighbors(entity_id)
    # Collect evidence for all claims
    evidence_map: dict[str, list] = {}
    for claim in claims:
        evs = graph.get_evidence_for_claim(claim.id)
        evidence_map[claim.id] = [ev.model_dump(mode="json") for ev in evs]
    return {
        "entity": entity.model_dump(mode="json"),
        "claims": [c.model_dump(mode="json") for c in claims],
        "evidence": evidence_map,
        "neighbors": [n.model_dump(mode="json") for n in neighbors],
    }
@app.get("/api/claims/{claim_id}")
async def api_claim_detail(claim_id: str):
    graph = get_graph()
    row = graph.conn.execute("SELECT * FROM claims WHERE id = ?", (claim_id,)).fetchone()
    if not row:
        return JSONResponse({"error": "Not found"}, status_code=404)
    claim = graph._row_to_claim(row)
    evidence = graph.get_evidence_for_claim(claim_id)
    subject = graph.get_entity(claim.subject_id)
    obj = graph.get_entity(claim.object_id) if claim.object_id else None
    return {
        "claim": claim.model_dump(mode="json"),
        "evidence": [ev.model_dump(mode="json") for ev in evidence],
        "subject": subject.model_dump(mode="json") if subject else None,
        "object": obj.model_dump(mode="json") if obj else None,
    }
@app.get("/api/merges")
async def api_merges(limit: int = Query(default=50, le=200)):
    graph = get_graph()
    merges = graph.get_merge_log()
    return [m.model_dump(mode="json") for m in merges[:limit]]
# -------------------------------------------------------------------
# API: Search / Retrieval
# -------------------------------------------------------------------
@app.get("/api/search")
async def api_search(
    q: str = Query(..., min_length=1),
    top_k: int = Query(default=10, le=50),
    include_historical: bool = False,
):
    """Search the memory graph and return a grounded context pack."""
    retriever = get_retriever()
    pack = retriever.search(q, top_k=top_k, include_historical=include_historical)
    return pack.model_dump(mode="json")
@app.get("/api/search/text")
async def api_search_text(
    q: str = Query(..., min_length=1),
    top_k: int = Query(default=10, le=50),
):
    """Search and return formatted text with citations."""
    retriever = get_retriever()
    pack = retriever.search(q, top_k=top_k)
    text = MemoryRetriever.format_context_pack(pack)
    return {"question": q, "formatted": text}
# -------------------------------------------------------------------
# Run
# -------------------------------------------------------------------
def run_server(host: str = "0.0.0.0", port: int = 8000) -> None:
    import uvicorn
    uvicorn.run(app, host=host, port=port)
if __name__ == "__main__":
    run_server()