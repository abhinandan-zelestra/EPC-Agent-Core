# EPC AgentCore — AI Query Pipeline

> Ask a plain-English question about the EPC project portfolio. Get a written answer, a data table, and a downloadable CSV — in seconds.

---

## Current Phase: Phase 2 — Integration & Validation ✅

```
Phase 1  ████████████  Complete   Agent 1 — SQL Generation
Phase 2  ████████████  Complete   Agent 2 + 3 — Validation, Execution, Narration
Phase 3  ░░░░░░░░░░░░  Next       Accuracy Evaluation & Live Testing
Phase 4  ░░░░░░░░░░░░  Pending    Deployment to Bedrock AgentCore
```

---

## What This System Does

A user sends a plain-English question — for example:

> *"What is the total CAPEX for all pipeline projects in Germany?"*

The pipeline returns:
- A **written narrative** summarising the key finding
- A **structured data object** with the full result set and summary stats
- A **downloadable CSV** link (S3 presigned URL, valid 1 hour)

No SQL knowledge required. No database access needed. No analyst in the loop.

---

## Architecture — Three Agents

```
User Question
     │
     ▼
┌─────────────────────────────────────────┐
│  AGENT 1 — SQL Generator                │  ← LLM (Claude Haiku)
│  Reads question + live Glue schema      │
│  + KB business rules → generates SQL    │
└──────────────────┬──────────────────────┘
                   │ sql_query + confidence_score
                   ▼
┌─────────────────────────────────────────┐
│  AGENT 2 — Validator                    │
│  Stage 1: Code checks every table and   │  ← Pure Python (sqlparse)
│           column against Glue schema    │
│           Hard gate — no LLM bypass     │
│  Stage 2: LLM fixes Athena dialect only │  ← LLM (Claude Haiku)
└──────────────────┬──────────────────────┘
                   │ validated + repaired SQL
                   ▼
┌─────────────────────────────────────────┐
│  AWS ATHENA                             │
│  Executes read-only query               │
│  Database: zelestra-epc (eu-central-1)  │
└──────────────────┬──────────────────────┘
                   │ raw rows
                   ▼
┌─────────────────────────────────────────┐
│  AGENT 3 — Narrator                     │
│  Formats rows → summary stats + sample  │  ← Pure Python
│  Writes plain-English narrative         │  ← LLM (Claude Haiku)
│  Exports CSV to S3, returns URL         │
└──────────────────┬──────────────────────┘
                   │
                   ▼
            Structured Response
```

---

## Project Structure

```
EPC_AgentCore/
│
├── main.py                        # Bedrock AgentCore entrypoint
├── orchestration.py               # Wires all 5 stages together
├── requirements.txt
│
├── agents/
│   ├── sql_generator.py           # Agent 1 — SQL generation (LLM)
│   ├── sql_validator.py           # Agent 2 — Whitelist check + dialect repair
│   └── narrator.py                # Agent 3 — Row formatter + narrative writer
│
├── services/
│   ├── glue_service.py            # Fetches live schema from AWS Glue (cached)
│   ├── kb_service.py              # Retrieves business rules from Bedrock KB
│   ├── athena_service.py          # Executes SQL, returns rows as list of dicts
│   └── s3_export_service.py       # Builds CSV in-memory, uploads to S3
│
├── test_sql_generator.py          # Agent 1 standalone test (34 questions)
├── test_pipeline.py               # Integration test — Agent 1, 2, 3 + E2E
├── eval_accuracy.py               # Accuracy evaluation — scored per agent
│
└── documentation/
    └── stakeholder_brief.docx     # Non-technical stakeholder summary
```

---

## Phase History

### Phase 1 — Agent 1: SQL Generation ✅ Complete

**Goal:** Given a plain-English question, generate syntactically correct Athena SQL using the live schema and business rules.

**What was built:**
- `agents/sql_generator.py` — LLM prompt with 12 SQL construction rules, versioned-table deduplication patterns, LATAM/region normalisation, budget H1/H2 logic
- `services/glue_service.py` — live schema fetch from AWS Glue
- `services/kb_service.py` — business context from Bedrock Knowledge Base (ID: `KKE3DDBSQG`)
- `test_sql_generator.py` — 34 test questions covering simple filters, JOINs, CTEs, financial assumptions, budget queries

**Issues found and fixed:**
- Claude occasionally returned SQL with raw newlines inside the JSON string (invalid JSON) → fixed with `_escape_newlines_in_strings()`
- Some outputs were double-encoded (entire JSON wrapped in extra escape layer) → fixed with `_robust_parse()` — 4-attempt fallback chain
- Both fixes confirmed against real failing cases from `test_results.csv`

---

### Phase 2 — Agents 2 & 3: Validation, Execution, Narration ✅ Complete

**Goal:** Validate the generated SQL before it reaches Athena, execute it safely, and return a human-readable answer.

**What was built:**

**Agent 2 (`agents/sql_validator.py`):**
- Stage 1 — deterministic whitelist check using `sqlparse`. Every table and column is extracted and checked against the Glue schema dict. Fake identifiers raise `ValueError` before any LLM call
- Stage 2 — narrow LLM call for Athena dialect repair only (date functions, identifier quoting). Forbidden from changing names or logic
- Confidence gate — rejects Agent 1 output below 0.7 before touching Athena

**Agent 3 (`agents/narrator.py`):**
- Formatter — structures raw Athena rows into `{ row_count, columns, summary_stats, sample, rows }`. Computes min/max/avg/sum for numeric columns
- Narrator — sends trimmed payload (sample only, not full rows) to Claude for plain-English summary. Full rows excluded to prevent context overflow on large result sets
- Language-aware — passes `language` parameter through to the narrator prompt

**Supporting services:**
- `services/athena_service.py` — polls Athena with 1.5s interval, 120s timeout, paginated result fetch
- `services/s3_export_service.py` — in-memory CSV build, S3 upload, presigned URL (1 hour TTL)
- `services/glue_service.py` — updated to expose `get_glue_schema_dict()` for Agent 2; single cached Glue API call per container lifetime

**Orchestration (`orchestration.py`):**  
Full 5-stage pipeline wired. Every stage returns a structured error with a `stage` field on failure so the caller always knows where it broke.

---

### Phase 3 — Accuracy Evaluation & Live Testing 🔄 In Progress

**Goal:** Measure how accurate each agent is against real data before deploying to production.

**What was built:**
- `eval_accuracy.py` — per-agent accuracy scorer:
  - Agent 1: 10 questions × 5 dimensions (correct tables, required keywords, forbidden patterns, intent match, structural validity)
  - Agent 2: Accept tests (valid SQL must pass + logic preserved after repair) + Reject tests (fake table, fake column, empty SQL, low confidence)
  - Agent 3: 6 scenarios with known mock rows — checks row_count, narrative accuracy (must mention key numbers/entities), no JSON leakage, empty result handling, language adaptation
- `test_pipeline.py` — integration test covering each agent in isolation and full E2E

**Remaining in this phase:**
- [ ] Run `eval_accuracy.py` against live Athena data
- [ ] Run `test_pipeline.py --agent e2e` with real questions
- [ ] Review accuracy scores and fix any Agent 1 prompt gaps

---

### Phase 4 — Deployment to Bedrock AgentCore ⏳ Pending

**Goal:** Deploy the containerised pipeline to AWS Bedrock AgentCore in eu-central-1.

**Prerequisites before deploy:**
- [ ] Create S3 bucket `zelestra-epc-exports` in eu-central-1
- [ ] IAM role must have: `bedrock:InvokeModel`, `athena:*`, `glue:GetTables`, `s3:PutObject` + `s3:GetObject` on both S3 buckets, `bedrock-agent-runtime:Retrieve`
- [ ] Bedrock model `anthropic.claude-3-haiku-20240307-v1:0` enabled in eu-central-1
- [ ] Knowledge Base `KKE3DDBSQG` accessible from the container role

**Deploy command (once prerequisites met):**
```bash
bedrock-agentcore deploy
```

---

## Running Locally (Phase 3 testing)

> Requires AWS credentials with access to Bedrock, Glue, Athena, and the Knowledge Base.

```bash
# Full pipeline — single question
python3 -c "
from orchestration import run_pipeline
import json
result = run_pipeline('How many projects are in Germany?')
print(json.dumps(result, indent=2))
"

# Agent 1 only
python3 test_sql_generator.py

# Integration tests
python3 test_pipeline.py              # all agents
python3 test_pipeline.py --agent 1    # Agent 1 only
python3 test_pipeline.py --agent 2    # Agent 2 only
python3 test_pipeline.py --agent 3    # Agent 3 only
python3 test_pipeline.py --agent e2e  # full pipeline

# Accuracy evaluation
python3 eval_accuracy.py              # all agents
python3 eval_accuracy.py --agent 1
python3 eval_accuracy.py --agent 2
python3 eval_accuracy.py --agent 3
```

---

## AWS Configuration

| Resource | Value |
|---|---|
| Region | eu-central-1 |
| Athena database | zelestra-epc |
| Athena output bucket | s3://aws-athena-query-results-eu-central-1-891377001420/ |
| Athena workgroup | primary |
| Bedrock Knowledge Base | KKE3DDBSQG |
| LLM model | anthropic.claude-3-haiku-20240307-v1:0 |
| Export bucket (Phase 4) | zelestra-epc-exports |

---

## Response Schema

A successful pipeline call returns:

```json
{
  "status": "ok",
  "query_intent": "Get all projects in Germany",
  "executed_sql": "SELECT ...",
  "narrative": "There are 12 active projects in Germany...",
  "structured_data": {
    "row_count": 12,
    "columns": ["project_name", "stage", "mwe"],
    "summary_stats": { "mwe": { "min": 50, "max": 300, "avg": 142, "sum": 1704 } },
    "sample": [ ... ],
    "rows": [ ... ]
  },
  "downloadable_csv": "https://s3.amazonaws.com/...",
  "agent1_meta": {
    "tables_used": ["projects"],
    "filters_applied": ["project_country = 'Germany'"],
    "confidence_score": 0.9
  }
}
```

On failure at any stage:

```json
{
  "status": "error",
  "stage": "agent2_validation",
  "message": "Whitelist check failed — Unknown table: 'fake_table'"
}
```