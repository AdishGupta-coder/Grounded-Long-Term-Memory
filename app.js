// Layer10 Memory Graph Explorer — Frontend
const API_BASE = '';
let network = null;
let graphData = { nodes: [], edges: [] };
let allNodes = [];
let allEdges = [];
let activeFilters = {
    entityTypes: new Set(),
    minConfidence: 0,
    showHistorical: false,
};
// -------------------------------------------------------------------
// Initialization
// -------------------------------------------------------------------
document.addEventListener('DOMContentLoaded', () => {
    loadStats();
    loadGraph();
    loadMerges();
    document.getElementById('search-input').addEventListener('keypress', (e) => {
        if (e.key === 'Enter') doSearch();
    });
    document.getElementById('confidence-slider').addEventListener('input', (e) => {
        activeFilters.minConfidence = parseInt(e.target.value) / 100;
        document.getElementById('confidence-value').textContent = e.target.value + '%';
        applyFilters();
    });
    document.getElementById('show-historical').addEventListener('change', (e) => {
        activeFilters.showHistorical = e.target.checked;
        applyFilters();
    });
});
// -------------------------------------------------------------------
// Data loading
// -------------------------------------------------------------------
async function loadStats() {
    try {
        const resp = await fetch(`${API_BASE}/api/stats`);
        const data = await resp.json();
        const s = data.stats;
        document.getElementById('stats-badge').textContent =
            `${s.canonical_entities} entities · ${s.current_claims} claims · ${s.evidence} evidence · ${s.merge_log} merges`;
        // Build entity type filters
        const container = document.getElementById('entity-type-filters');
        container.innerHTML = '';
        const colors = {
            person: '#4CAF50', repository: '#2196F3', issue: '#FF9800',
            pull_request: '#9C27B0', label: '#F44336', milestone: '#00BCD4', topic: '#FFC107',
        };
        for (const [type, count] of Object.entries(data.entity_types)) {
            activeFilters.entityTypes.add(type);
            const div = document.createElement('div');
            div.className = 'entity-type-checkbox';
            div.innerHTML = `
                <input type="checkbox" checked data-type="${type}" onchange="toggleEntityType('${type}', this.checked)">
                <span class="type-dot" style="background:${colors[type] || '#999'}"></span>
                <span>${type} (${count})</span>
            `;
            container.appendChild(div);
        }
    } catch (e) {
        console.error('Failed to load stats:', e);
    }
}
async function loadGraph() {
    try {
        const resp = await fetch(`${API_BASE}/api/graph?max_nodes=200&max_edges=400`);
        graphData = await resp.json();
        allNodes = graphData.nodes;
        allEdges = graphData.edges;
        renderGraph(allNodes, allEdges);
    } catch (e) {
        console.error('Failed to load graph:', e);
    }
}
async function loadMerges() {
    try {
        const resp = await fetch(`${API_BASE}/api/merges?limit=30`);
        const merges = await resp.json();
        const container = document.getElementById('merge-list');
        container.innerHTML = '';
        for (const m of merges) {
            const div = document.createElement('div');
            div.className = 'merge-item';
            div.innerHTML = `
                <span class="merge-type-badge merge-${m.merge_type}">${m.merge_type}</span>
                <div class="merge-reason">${escapeHtml(m.reason || '').substring(0, 100)}</div>
            `;
            container.appendChild(div);
        }
        if (merges.length === 0) {
            container.innerHTML = '<div style="color:#999;font-size:12px;">No merges recorded.</div>';
        }
    } catch (e) {
        console.error('Failed to load merges:', e);
    }
}
// -------------------------------------------------------------------
// Graph rendering
// -------------------------------------------------------------------
function renderGraph(nodes, edges) {
    const container = document.getElementById('graph-canvas');
    const data = {
        nodes: new vis.DataSet(nodes),
        edges: new vis.DataSet(edges),
    };
    const options = {
        physics: {
            enabled: true,
            solver: 'forceAtlas2Based',
            forceAtlas2Based: {
                gravitationalConstant: -30,
                centralGravity: 0.005,
                springLength: 120,
                springConstant: 0.05,
                damping: 0.4,
            },
            stabilization: { iterations: 150, fit: true },
        },
        nodes: {
            font: { size: 11, color: '#333' },
            borderWidth: 1,
            shadow: true,
        },
        edges: {
            font: { size: 9, color: '#777', align: 'middle' },
            smooth: { type: 'continuous' },
            width: 1,
        },
        interaction: {
            hover: true,
            tooltipDelay: 200,
            multiselect: false,
        },
        groups: {
            person: { shape: 'dot', color: '#4CAF50' },
            repository: { shape: 'diamond', color: '#2196F3' },
            issue: { shape: 'box', color: '#FF9800' },
            pull_request: { shape: 'box', color: '#9C27B0' },
            label: { shape: 'ellipse', color: '#F44336' },
            milestone: { shape: 'triangle', color: '#00BCD4' },
            topic: { shape: 'star', color: '#FFC107' },
        },
    };
    network = new vis.Network(container, data, options);
    network.on('click', (params) => {
        if (params.nodes.length > 0) {
            showEntityDetail(params.nodes[0]);
        } else if (params.edges.length > 0) {
            showClaimDetail(params.edges[0]);
        }
    });
}
// -------------------------------------------------------------------
// Filters
// -------------------------------------------------------------------
function toggleEntityType(type, checked) {
    if (checked) {
        activeFilters.entityTypes.add(type);
    } else {
        activeFilters.entityTypes.delete(type);
    }
    applyFilters();
}
function applyFilters() {
    const filteredNodes = allNodes.filter(n => activeFilters.entityTypes.has(n.entity_type));
    const nodeIds = new Set(filteredNodes.map(n => n.id));
    const filteredEdges = allEdges.filter(e => {
        if (!nodeIds.has(e.from) || !nodeIds.has(e.to)) return false;
        if (e.confidence < activeFilters.minConfidence) return false;
        if (!activeFilters.showHistorical && !e.is_current) return false;
        return true;
    });
    renderGraph(filteredNodes, filteredEdges);
}
function resetFilters() {
    activeFilters.minConfidence = 0;
    activeFilters.showHistorical = false;
    document.getElementById('confidence-slider').value = 0;
    document.getElementById('confidence-value').textContent = '0%';
    document.getElementById('show-historical').checked = false;
    // Re-check all entity types
    const checkboxes = document.querySelectorAll('#entity-type-filters input[type="checkbox"]');
    checkboxes.forEach(cb => {
        cb.checked = true;
        activeFilters.entityTypes.add(cb.dataset.type);
    });
    applyFilters();
}
// -------------------------------------------------------------------
// Detail panel
// -------------------------------------------------------------------
async function showEntityDetail(entityId) {
    try {
        const resp = await fetch(`${API_BASE}/api/entities/${entityId}`);
        const data = await resp.json();
        if (data.error) return;
        document.getElementById('detail-placeholder').style.display = 'none';
        document.getElementById('search-results').style.display = 'none';
        document.getElementById('detail-content').style.display = 'block';
        const entity = data.entity;
        document.getElementById('detail-title').textContent = entity.name;
        const typeBadge = document.getElementById('detail-type');
        typeBadge.textContent = entity.entity_type;
        typeBadge.className = `badge badge-${entity.entity_type}`;
        // Properties
        const propsDiv = document.getElementById('detail-properties');
        propsDiv.innerHTML = '';
        if (entity.aliases && entity.aliases.length > 0) {
            propsDiv.innerHTML += `<div class="prop-row"><span class="prop-key">Aliases</span><span class="prop-value">${escapeHtml(entity.aliases.join(', '))}</span></div>`;
        }
        if (entity.first_seen) {
            propsDiv.innerHTML += `<div class="prop-row"><span class="prop-key">First seen</span><span class="prop-value">${formatDate(entity.first_seen)}</span></div>`;
        }
        if (entity.properties) {
            for (const [k, v] of Object.entries(entity.properties)) {
                if (k === 'html_url') {
                    propsDiv.innerHTML += `<div class="prop-row"><span class="prop-key">${k}</span><span class="prop-value"><a href="${v}" target="_blank">Link</a></span></div>`;
                } else {
                    propsDiv.innerHTML += `<div class="prop-row"><span class="prop-key">${k}</span><span class="prop-value">${escapeHtml(String(v).substring(0, 80))}</span></div>`;
                }
            }
        }
        // Claims
        const claimsDiv = document.getElementById('detail-claims');
        claimsDiv.innerHTML = '';
        for (const claim of data.claims) {
            const historical = !claim.is_current ? 'claim-historical' : '';
            const evCount = (data.evidence[claim.id] || []).length;
            const div = document.createElement('div');
            div.className = `claim-item ${historical}`;
            div.innerHTML = `
                <div class="claim-predicate">${escapeHtml(claim.predicate)}</div>
                <div class="claim-value">${escapeHtml(claim.object_value || claim.object_id || '')}</div>
                <div class="claim-meta">
                    ${claim.claim_type} · conf: ${(claim.confidence * 100).toFixed(0)}% · ${evCount} evidence
                    ${!claim.is_current ? ' · HISTORICAL' : ''}
                </div>
                <div class="confidence-bar"><div class="confidence-fill" style="width:${claim.confidence * 100}%"></div></div>
            `;
            div.onclick = () => showClaimEvidence(claim.id, data.evidence[claim.id] || []);
            claimsDiv.appendChild(div);
        }
        // Neighbors
        const neighborsDiv = document.getElementById('detail-neighbors');
        neighborsDiv.innerHTML = '';
        for (const neighbor of data.neighbors) {
            const div = document.createElement('div');
            div.className = 'neighbor-item';
            div.innerHTML = `
                <div class="neighbor-name">${escapeHtml(neighbor.name)}</div>
                <div class="neighbor-type">${neighbor.entity_type}</div>
            `;
            div.onclick = () => {
                showEntityDetail(neighbor.id);
                if (network) network.selectNodes([neighbor.id]);
            };
            neighborsDiv.appendChild(div);
        }
    } catch (e) {
        console.error('Failed to load entity detail:', e);
    }
}
function showClaimEvidence(claimId, evidenceList) {
    const evidenceDiv = document.getElementById('detail-evidence');
    evidenceDiv.innerHTML = '';
    for (const ev of evidenceList) {
        const div = document.createElement('div');
        div.className = 'evidence-item';
        div.innerHTML = `
            <div class="evidence-excerpt">"${escapeHtml(ev.excerpt.substring(0, 300))}"</div>
            <div class="evidence-source">
                Source: ${escapeHtml(ev.source_id)}
                ${ev.source_url ? `· <a href="${ev.source_url}" target="_blank">View original</a>` : ''}
                ${ev.timestamp ? `· ${formatDate(ev.timestamp)}` : ''}
            </div>
        `;
        evidenceDiv.appendChild(div);
    }
    if (evidenceList.length === 0) {
        evidenceDiv.innerHTML = '<div style="color:#999;font-size:12px;">No evidence linked.</div>';
    }
}
async function showClaimDetail(edgeId) {
    // Find the edge data
    const edge = allEdges.find(e => {
        // vis.js may use numeric edge IDs
        return true; // We'll use the claim_id from the edge data
    });
    // For edge clicks, find the claim_id from the edge
    const edgeData = allEdges.find(e => e.claim_id);
    if (edgeData) {
        try {
            const resp = await fetch(`${API_BASE}/api/claims/${edgeData.claim_id}`);
            const data = await resp.json();
            if (data.claim) {
                document.getElementById('detail-placeholder').style.display = 'none';
                document.getElementById('search-results').style.display = 'none';
                document.getElementById('detail-content').style.display = 'block';
                document.getElementById('detail-title').textContent = data.claim.predicate;
                const typeBadge = document.getElementById('detail-type');
                typeBadge.textContent = data.claim.claim_type;
                typeBadge.className = 'badge badge-issue';
                showClaimEvidence(data.claim.id, data.evidence || []);
            }
        } catch (e) {
            console.error('Failed to load claim detail:', e);
        }
    }
}
// -------------------------------------------------------------------
// Search
// -------------------------------------------------------------------
async function doSearch() {
    const query = document.getElementById('search-input').value.trim();
    if (!query) return;
    try {
        const resp = await fetch(`${API_BASE}/api/search?q=${encodeURIComponent(query)}&top_k=10`);
        const pack = await resp.json();
        document.getElementById('detail-placeholder').style.display = 'none';
        document.getElementById('detail-content').style.display = 'none';
        document.getElementById('search-results').style.display = 'block';
        // Entities
        const entDiv = document.getElementById('search-entities');
        entDiv.innerHTML = '<h4>Entities</h4>';
        for (const e of pack.entities || []) {
            const div = document.createElement('div');
            div.className = 'search-result-item';
            div.innerHTML = `
                <span class="badge badge-${e.entity_type}" style="font-size:10px;">${e.entity_type}</span>
                <strong>${escapeHtml(e.name)}</strong>
            `;
            div.onclick = () => {
                showEntityDetail(e.id);
                if (network) {
                    network.selectNodes([e.id]);
                    network.focus(e.id, { animation: true, scale: 1.5 });
                }
            };
            entDiv.appendChild(div);
        }
        // Claims
        const claimDiv = document.getElementById('search-claims');
        claimDiv.innerHTML = '<h4>Claims</h4>';
        for (const c of (pack.claims || []).slice(0, 10)) {
            const div = document.createElement('div');
            div.className = 'search-result-item';
            div.innerHTML = `
                <div class="claim-predicate">${escapeHtml(c.predicate)}</div>
                <div class="claim-value">${escapeHtml(c.object_value || c.object_id || '')}</div>
                <div class="claim-meta">${c.claim_type} · conf: ${(c.confidence * 100).toFixed(0)}%</div>
            `;
            claimDiv.appendChild(div);
        }
        // Evidence
        const evDiv = document.getElementById('search-evidence');
        evDiv.innerHTML = '<h4>Evidence</h4>';
        for (const ev of (pack.evidence || []).slice(0, 8)) {
            const div = document.createElement('div');
            div.className = 'search-result-item';
            div.innerHTML = `
                <div class="evidence-excerpt">"${escapeHtml(ev.excerpt.substring(0, 200))}"</div>
                <div class="evidence-source">
                    ${escapeHtml(ev.source_id)}
                    ${ev.source_url ? `· <a href="${ev.source_url}" target="_blank">View</a>` : ''}
                </div>
            `;
            evDiv.appendChild(div);
        }
    } catch (e) {
        console.error('Search failed:', e);
    }
}
// -------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------
function escapeHtml(str) {
    if (!str) return '';
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}
function formatDate(isoString) {
    if (!isoString) return '';
    try {
        const d = new Date(isoString);
        return d.toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' });
    } catch {
        return isoString;
    }
}