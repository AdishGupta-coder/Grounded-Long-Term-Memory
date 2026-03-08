# Layer10 Take-Home Project  
**Grounded Long-Term Memory via Structured Extraction, Deduplication, and a Context Graph**

This project implements an end-to-end pipeline that converts unstructured GitHub issue discussions into a **grounded long-term memory graph** consisting of entities, claims, and evidence.

The system demonstrates how scattered project communication can be transformed into structured knowledge that remains **traceable, deduplicated, and queryable over time**, which mirrors Layer10's goal of building reliable organizational memory.

---

# 1. Overview

The pipeline performs the following steps:

1. **Corpus Acquisition**
2. **Structured Extraction**
3. **Deduplication and Canonicalization**
4. **Memory Graph Construction**
5. **Retrieval with Grounded Evidence**
6. **Visualization API**

All extracted knowledge is stored in a **SQLite-backed graph store** and can be queried using a hybrid search system.

---

# 2. Corpus Used

### Dataset
This project uses **GitHub Issues from the React repository**:

Repository:
```
facebook/react
```

### Source
Data is fetched directly from the **GitHub REST API**.

Example endpoint:
```
https://api.github.com/repos/facebook/react/issues
```

### What is downloaded
For each issue:

- Issue metadata
- Issue author
- Labels
- State changes
- Milestone
- Issue body
- All comments

### Reproducing the download

Run:

```bash
python run_pipeline.py --repo facebook/react --count 150
```

Optional environment variable to avoid rate limits:

```bash
export GITHUB_TOKEN=your_token
```

The downloader fetches issues using the GitHub API and stores them locally for processing.

The code responsible is:

```
corpus.py
```

Function:

```
download_corpus(repo, count)
```

This function:
- calls GitHub REST endpoints
- retrieves issue metadata and comments
- writes normalized JSON files for downstream processing

---

# 3. Project Structure

```
project/
│
├── corpus.py
├── extraction.py
├── dedup.py
├── graph.py
├── retrieval.py
├── schema.py
├── run_pipeline.py
│
├── server.py
├── index.html
├── styles.css
├── app.js
│
└── outputs/
```

---

# 4. Ontology / Schema Design

The ontology is defined in:

```
schema.py
```

### Entity Types

Entities represent real-world objects:

```
PERSON
REPOSITORY
ISSUE
PULL_REQUEST
LABEL
MILESTONE
TOPIC
```

Example:

```
Person: "gaearon"
Issue: #24289
Label: "bug"
Topic: "hook dependency issue"
```

### Claim Types

Claims represent relationships between entities.

Examples:

```
AUTHORSHIP
ASSIGNMENT
LABELING
STATUS_CHANGE
REFERENCE
MILESTONE_MEMBERSHIP
DISCUSSION_TOPIC
```

Example claim:

```
(Person) gaearon
    └── authored
          (Issue) #14239
```

---

# 5. Evidence Grounding

Every claim must be backed by **evidence**.

Evidence contains:

```
source_id
source_type
excerpt
timestamp
offsets
```

Example:

```
Claim:
  Issue #12034 labeled "bug"

Evidence:
  source: GitHub Issue
  excerpt: "Labels: bug"
  timestamp: 2024-02-10
```

This guarantees that every piece of memory can be traced back to the **exact source text**.

---

# 6. Extraction System

Extraction logic is implemented in:

```
extraction.py
```

### Hybrid Extraction Approach

The system uses two techniques:

#### 1. Rule-Based Extraction

For structured GitHub fields:

Extracts:

- author
- labels
- assignees
- milestones
- issue state
- references

Example function:

```
extract_issue(issue_json)
```

This produces:

```
entities
claims
evidence
```

#### 2. LLM-Based Extraction

For unstructured text (issue body and comments).

The system calls an **Ollama local model**:

```
qwen2.5:3b
```

Used for extracting:

- discussion topics
- decisions
- references between issues

This step converts natural language into structured claims.

---

# 7. Validation and Repair

The extraction system includes several safety mechanisms.

### Deterministic IDs

Stable IDs are generated using:

```
_stable_id()
```

This ensures reproducibility.

Example:

```
hash(issue_id + author + timestamp)
```

### Schema Validation

All extracted objects are validated using **Pydantic models**.

If extraction produces invalid data:

- the object is discarded
- fallback rules are applied

### Extraction Versioning

Each claim stores:

```
extraction_version
```

Example:

```
v1
```

This allows **future schema changes with backfilling**.

---

# 8. Deduplication and Canonicalization

Deduplication logic is implemented in:

```
dedup.py
```

The system deduplicates at **three levels**.

---

## 8.1 Artifact Deduplication

Detects duplicate messages or issues.

Technique:

```
Jaccard similarity
```

Function:

```
_jaccard_similarity(text_a, text_b)
```

Near-identical messages (forwarded issues or copied comments) are merged.

---

## 8.2 Entity Canonicalization

Entities referring to the same object are merged.

Examples:

```
gaearon
dan_abramov
dan-abramov
```

Aliases are merged into a **canonical entity**.

Entity fields:

```
canonical_id
aliases
```

---

## 8.3 Claim Deduplication

Multiple mentions of the same fact are merged.

Example:

```
Issue #123 labeled bug
Issue #123 labeled bug
Issue #123 labeled bug
```

These become:

```
1 canonical claim
+ multiple evidence references
```

---

## 8.4 Reversible Merges

All merges are stored as:

```
MergeRecord
```

Fields include:

```
merge_type
source_ids
canonical_id
timestamp
reason
```

This ensures merges can be:

- audited
- reversed

---

# 9. Memory Graph Design

The memory graph is implemented in:

```
graph.py
```

The graph is stored in **SQLite**.

### Tables

```
entities
claims
evidence
merge_records
```

---

### Entity Table

Stores:

```
id
canonical_id
entity_type
name
aliases
properties
first_seen
last_seen
```

---

### Claim Table

Stores relationships:

```
subject
predicate
object
confidence
valid_from
valid_until
is_current
extraction_version
```

---

### Time Representation

The graph distinguishes between:

```
event_time
validity_time
```

Example:

```
Issue closed in 2022
Reopened in 2023
```

The system marks which claim is **currently valid**.

---

# 10. Incremental Updates

The pipeline is designed to support **continuous ingestion**.

Mechanisms:

### Idempotent Processing

Stable IDs ensure reprocessing does not duplicate objects.

### Versioning

When ontology changes:

```
re-run extraction
update graph
backfill claims
```

### Handling Edits and Deletes

Future improvements:

- mark claims invalid when source removed
- maintain historical validity windows

---

# 11. Retrieval and Grounding

Retrieval is implemented in:

```
retrieval.py
```

Class:

```
MemoryRetriever
```

---

## 11.1 Query → Candidate Mapping

The system uses **hybrid search**.

Methods:

1. Keyword search
2. TF-IDF vector similarity

Libraries:

```
scikit-learn
```

Key components:

```
TfidfVectorizer
cosine_similarity
```

Indexes built for:

```
entities
claims
evidence
```

---

## 11.2 Graph Expansion

After candidate retrieval, the system expands context:

```
entity → related claims → supporting evidence
```

To prevent explosion:

- limit top-k entities
- prune low-confidence claims
- prioritize recent evidence
- enforce diversity

---

## 11.3 Grounded Context Pack

Each answer returns a **ContextPack** containing:

```
entities
claims
evidence snippets
citations
```

Example:

```
Claim:
Issue #321 assigned to gaearon

Evidence:
Excerpt: "Assigning to @gaearon for investigation"
Source: GitHub comment
```

---

## 11.4 Handling Conflicts

When multiple claims conflict:

The system may:

- show both
- rank by confidence
- prefer newer claims

Example:

```
Issue status: open
Issue status: closed
```

Both appear with timestamps.

---

# 12. Visualization Layer

Visualization is implemented using:

```
FastAPI + HTML + JavaScript
```

Files:

```
server.py
index.html
app.js
styles.css
```

The UI allows users to:

- explore entities
- inspect claims
- view evidence excerpts
- inspect merged duplicates

API endpoints expose graph queries and search results.

---

# 13. Running the System

Install dependencies:

```
pip install -r requirements.txt
```

Run pipeline:

```
python run_pipeline.py
```

Optional flags:

```
--repo facebook/react
--count 150
--no-llm
--serve
```

Start API server:

```
python server.py
```

Then open:

```
http://localhost:8000
```

---

# 14. Example Retrieval Queries

Example questions:

```
Who authored issue #123?
Which labels are common in React issues?
What discussions reference issue #500?
```

The system returns grounded evidence snippets.

---

# 15. Adapting This System to Layer10

Layer10 targets **organizational knowledge across communication tools**.

Sources include:

```
Email
Slack / Teams
Docs
Jira / Linear
```

The current GitHub-based ontology would be extended.

---

## Ontology Changes

New entity types:

```
USER
TEAM
CHANNEL
DOCUMENT
TASK
PROJECT
```

New claim types:

```
decision_made
task_assigned
deadline_set
document_updated
conversation_reference
```

---

## Extraction Contract Changes

Extraction must support:

- threaded conversations
- quoted email chains
- edited messages
- attachments

Each claim must reference:

```
message_id
thread_id
timestamp
author
```

---

## Deduplication Strategy

Additional dedup challenges:

```
email forwards
Slack reposts
copy-pasted documents
ticket duplication
```

Solutions:

- thread reconstruction
- semantic similarity
- author-based identity resolution

---

## Grounding Requirements

All memory must remain traceable to original sources.

Each claim must store:

```
source system
message id
workspace id
visibility permissions
```

This prevents retrieving memory from sources the user cannot access.

---

## Long-Term Memory Behavior

Layer10 requires memory that remains correct over time.

Key features:

```
claim validity windows
superseded decisions
historical audit trail
source deletion propagation
```

For example:

```
Decision A approved
Later reversed
```

Both remain stored but only the latest is marked **current**.

---

# 16. Observability

The system should log:

```
extraction success rate
dedup merge rates
claim confidence distribution
retrieval latency
```

This helps detect degradation in extraction quality.

---

# 17. Summary

This project demonstrates a full pipeline for transforming raw communication data into **grounded organizational memory**.

Key contributions:

- structured extraction from unstructured discussions
- multi-level deduplication
- reversible canonicalization
- evidence-grounded knowledge graph
- hybrid retrieval with traceable citations
- visualization for auditing and exploration

The architecture is designed to scale to **Layer10’s multi-source enterprise knowledge environment**.