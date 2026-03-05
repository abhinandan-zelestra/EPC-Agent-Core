"""
Accuracy Evaluation — Agent 1, 2, and 3
=========================================
Measures how ACCURATE each agent is, not just whether it runs.

  Agent 1 accuracy  = SQL quality: does it correctly target the right tables,
                      columns, filters, and aggregations for each question?
                      Scored across 5 dimensions per question.

  Agent 2 accuracy  = Validation quality: does it correctly ACCEPT valid SQL
                      and correctly REJECT invalid SQL? Also checks that
                      dialect repair does not corrupt query logic.

  Agent 3 accuracy  = Narrative quality: does the narrative actually reflect
                      the data rows? Are numbers, counts, and key facts mentioned?

HOW TO RUN:
    python3 eval_accuracy.py             # evaluate all agents
    python3 eval_accuracy.py --agent 1   # Agent 1 only
    python3 eval_accuracy.py --agent 2   # Agent 2 only
    python3 eval_accuracy.py --agent 3   # Agent 3 only

OUTPUT:
    - Colour-coded per-question results in the terminal
    - Per-agent accuracy score (0–100%)
    - eval_accuracy_report.csv saved alongside this file
"""

import sys
import json
import csv
import re
import argparse
import traceback
from datetime import datetime
from typing import Any, Dict, List, Tuple

# ── colour helpers ────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
BLUE   = "\033[94m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def hdr(msg):  print(f"\n{BOLD}{BLUE}{'═'*65}{RESET}\n{BOLD}  {msg}{RESET}\n{'═'*65}")
def sub(msg):  print(f"\n{CYAN}  ▶ {msg}{RESET}")
def good(msg): print(f"    {GREEN}✔{RESET}  {msg}")
def bad(msg):  print(f"    {RED}✘{RESET}  {msg}")
def note(msg): print(f"    {YELLOW}~{RESET}  {msg}")

def safe_run(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs), None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"

# CSV report rows accumulated throughout
_report_rows: List[Dict] = []

def log(agent, question, dimension, score, max_score, detail):
    _report_rows.append({
        "agent":     agent,
        "question":  question[:80],
        "dimension": dimension,
        "score":     score,
        "max_score": max_score,
        "pct":       f"{round(100*score/max_score)}%" if max_score else "N/A",
        "detail":    detail[:120],
    })


# ══════════════════════════════════════════════════════════════════════════════
#  AGENT 1 — SQL GENERATION ACCURACY
# ══════════════════════════════════════════════════════════════════════════════
#
# Each test case defines:
#   question        — the user question
#   must_tables     — tables that MUST appear in the SQL
#   must_keywords   — SQL keywords / patterns that MUST appear
#   must_not        — things that must NOT appear (wrong tables, wrong logic)
#   intent_keywords — words that should appear in query_intent
#   description     — what we're testing
#
AGENT1_CASES = [
    {
        "description": "Simple project list by country",
        "question":    "Give me all projects in Germany",
        "must_tables": ["projects"],
        "must_keywords": ["germany", "project_country"],
        "must_not":    ["budget_details", "project_assumptions"],
        "intent_keywords": ["germany", "project"],
    },
    {
        "description": "Count projects per country",
        "question":    "How many projects do we have per country?",
        "must_tables": ["projects"],
        "must_keywords": ["count", "project_country", "group by"],
        "must_not":    [],
        "intent_keywords": ["count", "country"],
    },
    {
        "description": "Budget query with year filter",
        "question":    "Give me the development budget for Germany in 2025",
        "must_tables": ["budget_details", "projects"],
        "must_keywords": ["germany", "2025", "budget"],
        "must_not":    [],
        "intent_keywords": ["budget", "germany"],
    },
    {
        "description": "Latest-row deduplication pattern",
        "question":    "What is the latest budget for all projects?",
        "must_tables": ["budget_details"],
        "must_keywords": ["row_number", "partition by", "order by", "rn = 1"],
        "must_not":    [],
        "intent_keywords": ["budget", "latest"],
    },
    {
        "description": "Financial assumptions with CAPEX group",
        "question":    "What is the total CAPEX for all projects in Spain?",
        "must_tables": ["project_assumptions"],
        "must_keywords": ["capex", "spain"],
        "must_not":    [],
        "intent_keywords": ["capex", "spain"],
    },
    {
        "description": "H1 budget filter (month_index 1-6)",
        "question":    "Give me the H1 budget for Germany for 2025",
        "must_tables": ["budget_details"],
        "must_keywords": ["month_index", "germany", "2025"],
        "must_not":    [],
        "intent_keywords": ["h1", "budget", "germany"],
    },
    {
        "description": "LATAM region normalisation",
        "question":    "List all LATAM projects",
        "must_tables": ["projects"],
        "must_keywords": ["ecuador", "peru", "chile", "colombia"],
        "must_not":    ["latam"],   # should expand, not pass raw 'latam'
        "intent_keywords": ["latam", "project"],
    },
    {
        "description": "Pipeline project definition",
        "question":    "Show me all pipeline projects",
        "must_tables": ["projects"],
        "must_keywords": ["project_status", "developing"],
        "must_not":    [],
        "intent_keywords": ["pipeline"],
    },
    {
        "description": "Exclude abandoned/sold by default",
        "question":    "List all active projects",
        "must_tables": ["projects"],
        "must_keywords": ["abandoned", "sold"],   # exclusion must be explicit
        "must_not":    [],
        "intent_keywords": ["project"],
    },
    {
        "description": "COUNT DISTINCT on site_name",
        "question":    "How many projects are in AdvancedDevelopment?",
        "must_tables": ["projects"],
        "must_keywords": ["count", "distinct", "advanceddevelopment"],
        "must_not":    [],
        "intent_keywords": ["advanceddevelopment", "count"],
    },
]


def score_agent1_case(result: dict, case: dict) -> Tuple[int, int, List[str]]:
    """
    Score one Agent 1 result against expected criteria.
    Returns (points_earned, points_possible, notes).
    """
    earned = 0
    possible = 0
    notes = []

    sql   = result.get("sql_query", "").lower()
    intent = result.get("query_intent", "").lower()

    # ── Dimension 1: required tables (2 pts each) ──────────────────────────
    for tbl in case["must_tables"]:
        possible += 2
        if tbl.lower() in sql:
            earned += 2
            notes.append(f"✔ table '{tbl}' present")
        else:
            notes.append(f"✘ table '{tbl}' MISSING from SQL")

    # ── Dimension 2: required SQL keywords/patterns (1 pt each) ───────────
    for kw in case["must_keywords"]:
        possible += 1
        if kw.lower() in sql:
            earned += 1
            notes.append(f"✔ keyword '{kw}' present")
        else:
            notes.append(f"✘ keyword '{kw}' MISSING")

    # ── Dimension 3: forbidden patterns (2 pts each) ───────────────────────
    for bad_kw in case["must_not"]:
        possible += 2
        if bad_kw.lower() not in sql:
            earned += 2
            notes.append(f"✔ forbidden '{bad_kw}' correctly absent")
        else:
            notes.append(f"✘ forbidden '{bad_kw}' FOUND — logic error")

    # ── Dimension 4: intent description matches (1 pt each) ───────────────
    for kw in case["intent_keywords"]:
        possible += 1
        if kw.lower() in intent:
            earned += 1
            notes.append(f"✔ intent contains '{kw}'")
        else:
            notes.append(f"~ intent missing '{kw}' (got: {intent[:60]})")

    # ── Dimension 5: structural validity (3 pts) ──────────────────────────
    possible += 3
    if "select" in sql and "from" in sql:
        earned += 1
        notes.append("✔ SQL has SELECT...FROM")
    else:
        notes.append("✘ SQL missing SELECT or FROM")

    if result.get("confidence_score", 0) >= 0.7:
        earned += 1
        notes.append(f"✔ confidence ≥ 0.7 ({result.get('confidence_score')})")
    else:
        notes.append(f"✘ confidence < 0.7 ({result.get('confidence_score')})")

    if result.get("tables_used"):
        earned += 1
        notes.append(f"✔ tables_used populated: {result.get('tables_used')}")
    else:
        notes.append("✘ tables_used empty")

    return earned, possible, notes


def run_agent1_accuracy():
    hdr("AGENT 1 — SQL Generation Accuracy")

    from agents.sql_generator import generate_sql
    from services.glue_service import get_glue_schema
    from services.kb_service import retrieve_kb_context

    print("  Fetching schema and KB context...")
    kb     = retrieve_kb_context("projects")
    schema = get_glue_schema("zelestra-epc")
    context = {
        "schema": kb + "\n" + schema,
        "business_rules": (
            "Exclude Abandoned/Sold projects by default.\n"
            "Always use COUNT(DISTINCT site_name) when counting projects."
        )
    }

    total_earned  = 0
    total_possible = 0

    for i, case in enumerate(AGENT1_CASES, 1):
        sub(f"[{i}/{len(AGENT1_CASES)}] {case['description']}")
        print(f"    Q: {case['question']}")

        result, err = safe_run(generate_sql, case["question"], context=context)

        if err or (result and "error" in result):
            error_msg = err or result.get("error", "unknown")
            bad(f"generate_sql() failed: {error_msg[:80]}")
            log("Agent1", case["question"], "CRASH", 0, 10, error_msg)
            total_possible += 10
            continue

        earned, possible, notes = score_agent1_case(result, case)
        total_earned   += earned
        total_possible += possible
        pct = round(100 * earned / possible) if possible else 0

        for n in notes:
            if n.startswith("✔"):  good(n[2:])
            elif n.startswith("✘"): bad(n[2:])
            else:                   note(n[2:])

        print(f"    {BOLD}Score: {earned}/{possible}  ({pct}%){RESET}")
        log("Agent1", case["question"], "OVERALL", earned, possible,
            f"sql_preview={result.get('sql_query','')[:60]}")

    overall_pct = round(100 * total_earned / total_possible) if total_possible else 0
    print(f"\n  {BOLD}Agent 1 Overall Accuracy: {total_earned}/{total_possible} = {overall_pct}%{RESET}")
    return overall_pct


# ══════════════════════════════════════════════════════════════════════════════
#  AGENT 2 — VALIDATION ACCURACY
# ══════════════════════════════════════════════════════════════════════════════
#
# Two categories:
#   ACCEPT cases — valid SQL that Agent 2 must NOT reject
#   REJECT cases — invalid SQL that Agent 2 MUST reject
#
# For ACCEPT cases we also check that the repaired SQL still contains
# the key logic from the original (dialect repair must not corrupt queries).
#
def build_agent2_cases(schema_dict: dict):
    """Build test cases using the real Glue schema so table/col names are real."""
    tables    = list(schema_dict.keys())
    t1        = tables[0] if tables else "projects"
    t1_cols   = schema_dict.get(t1, ["record_id", "site_name"])
    c1, c2    = t1_cols[0], t1_cols[1] if len(t1_cols) > 1 else t1_cols[0]

    # Find a second table if available
    t2        = tables[1] if len(tables) > 1 else t1
    t2_cols   = schema_dict.get(t2, ["record_id"])
    c2t2      = t2_cols[0]

    accept_cases = [
        {
            "description": "Simple SELECT — should pass",
            "sql":   f"SELECT {c1}, {c2} FROM {t1} LIMIT 10",
            "check_preserved": [c1, c2, t1],
        },
        {
            "description": "SELECT with WHERE — should pass",
            "sql":   f"SELECT {c1} FROM {t1} WHERE {c2} = 'test' LIMIT 5",
            "check_preserved": [c1, t1],
        },
        {
            "description": "COUNT with GROUP BY — should pass",
            "sql":   f"SELECT {c1}, COUNT(*) AS cnt FROM {t1} GROUP BY {c1}",
            "check_preserved": ["count", t1, c1],
        },
        {
            "description": "CTE with ROW_NUMBER — should pass",
            "sql": (
                f"WITH latest AS ("
                f" SELECT *, ROW_NUMBER() OVER (PARTITION BY {c1} ORDER BY {c1} DESC) AS rn"
                f" FROM {t1}"
                f") SELECT {c1} FROM latest WHERE rn = 1"
            ),
            "check_preserved": ["row_number", t1, "rn = 1"],
        },
        {
            "description": "Two-table JOIN — should pass",
            "sql":   f"SELECT a.{c1}, b.{c2t2} FROM {t1} a JOIN {t2} b ON a.{c1} = b.{c1} LIMIT 10",
            "check_preserved": [t1, t2],
        },
    ]

    reject_cases = [
        {
            "description": "Completely fake table — must reject",
            "sql":    "SELECT id FROM totally_fake_table_xyz WHERE year = 2025",
            "reason": "fake table",
        },
        {
            "description": "Real table but fake column — must reject",
            "sql":    f"SELECT nonexistent_column_abc123 FROM {t1}",
            "reason": "fake column",
        },
        {
            "description": "Empty SQL — must reject",
            "sql":    "",
            "reason": "empty sql",
        },
        {
            "description": "Low confidence (0.1) — must reject",
            "sql":    f"SELECT {c1} FROM {t1}",
            "confidence_override": 0.1,
            "reason": "low confidence",
        },
    ]

    return accept_cases, reject_cases


def run_agent2_accuracy():
    hdr("AGENT 2 — Validation Accuracy")

    try:
        from agents.sql_validator import validate_and_repair, CONFIDENCE_THRESHOLD
        from services.glue_service import get_glue_schema_dict
    except ImportError as e:
        bad(f"Cannot import Agent 2: {e}")
        log("Agent2", "import", "IMPORT_ERROR", 0, 1, str(e))
        return 0

    print("  Fetching Glue schema dict...")
    schema_dict, err = safe_run(get_glue_schema_dict, "zelestra-epc")
    if err or not schema_dict:
        bad(f"Could not fetch schema dict: {err}")
        return 0

    accept_cases, reject_cases = build_agent2_cases(schema_dict)

    total_earned   = 0
    total_possible = 0

    # ── ACCEPT cases ──────────────────────────────────────────────────────────
    print(f"\n  {BOLD}── ACCEPT cases (valid SQL must not be rejected) ──{RESET}")

    for i, case in enumerate(accept_cases, 1):
        sub(f"[Accept {i}/{len(accept_cases)}] {case['description']}")
        print(f"    SQL: {case['sql'][:80]}")

        agent1_mock = {
            "sql_query":        case["sql"],
            "confidence_score": 0.9,
            "query_intent":     "test",
            "tables_used":      [],
            "filters_applied":  [],
            "business_rules_applied": [],
        }

        result, err = safe_run(validate_and_repair, agent1_mock, schema_dict)

        # 2 pts: did not reject valid SQL
        possible = 2
        if err is None and isinstance(result, str) and len(result) > 0:
            earned = 2
            good(f"Correctly accepted — returned repaired SQL")
        else:
            earned = 0
            bad(f"Valid SQL was incorrectly rejected: {err}")

        # 2 pts: key logic preserved after dialect repair
        preserved_pts = 0
        for token in case.get("check_preserved", []):
            possible += 1
            if result and token.lower() in result.lower():
                preserved_pts += 1
                good(f"Logic preserved: '{token}' still in repaired SQL")
            else:
                bad(f"Logic LOST after repair: '{token}' missing from repaired SQL")

        earned += preserved_pts
        total_earned   += earned
        total_possible += possible
        pct = round(100 * earned / possible) if possible else 0
        print(f"    {BOLD}Score: {earned}/{possible} ({pct}%){RESET}")
        log("Agent2", case["description"], "ACCEPT", earned, possible,
            result[:60] if result else str(err)[:60])

    # ── REJECT cases ──────────────────────────────────────────────────────────
    print(f"\n  {BOLD}── REJECT cases (invalid SQL must be caught) ──{RESET}")

    for i, case in enumerate(reject_cases, 1):
        sub(f"[Reject {i}/{len(reject_cases)}] {case['description']}")

        agent1_mock = {
            "sql_query":        case["sql"],
            "confidence_score": case.get("confidence_override", 0.9),
            "query_intent":     "test",
            "tables_used":      [],
            "filters_applied":  [],
            "business_rules_applied": [],
        }

        result, err = safe_run(validate_and_repair, agent1_mock, schema_dict)

        possible = 3
        if err is not None:
            earned = 3
            good(f"Correctly rejected ({case['reason']}): {err[:70]}")
        else:
            earned = 0
            bad(f"FAILED to reject — {case['reason']} passed through to Athena!")

        total_earned   += earned
        total_possible += possible
        pct = round(100 * earned / possible) if possible else 0
        print(f"    {BOLD}Score: {earned}/{possible} ({pct}%){RESET}")
        log("Agent2", case["description"], "REJECT", earned, possible,
            err[:60] if err else "NOT REJECTED")

    overall_pct = round(100 * total_earned / total_possible) if total_possible else 0
    print(f"\n  {BOLD}Agent 2 Overall Accuracy: {total_earned}/{total_possible} = {overall_pct}%{RESET}")
    return overall_pct


# ══════════════════════════════════════════════════════════════════════════════
#  AGENT 3 — NARRATIVE ACCURACY
# ══════════════════════════════════════════════════════════════════════════════
#
# For each test we provide known mock rows and check whether the narrative
# accurately reflects: key numbers, entity names, and the right conclusion.
# We also check formatting quality (length, no JSON leakage, language match).
#
AGENT3_CASES = [
    {
        "description":    "Project count by country",
        "intent":         "Count projects per country",
        "language":       "en",
        "rows": [
            {"project_country": "Germany", "project_count": "12"},
            {"project_country": "Spain",   "project_count": "8"},
            {"project_country": "Italy",   "project_count": "5"},
        ],
        # Numbers and names that MUST appear in the narrative
        "must_mention":   ["12", "germany", "8", "spain"],
        # Things that must NOT appear (JSON leakage, raw column names)
        "must_not_contain": ["{", "project_count", "row_count"],
        "min_length":     40,
        "max_length":     800,
    },
    {
        "description":    "Single project financials",
        "intent":         "Get CAPEX for Aurora project",
        "language":       "en",
        "rows": [
            {"project_name": "Aurora", "total_capex": "145000000", "irr": "12.5"},
        ],
        "must_mention":   ["aurora", "145000000", "12.5"],
        "must_not_contain": ["{", "total_capex"],
        "min_length":     30,
        "max_length":     600,
    },
    {
        "description":    "Empty result set",
        "intent":         "Find projects in Antarctica",
        "language":       "en",
        "rows":           [],
        # Should acknowledge no results
        "must_mention":   ["no ", "not found", "0", "none", "empty", "result"],
        "must_mention_any": True,   # only need ONE of these
        "must_not_contain": ["{"],
        "min_length":     15,
        "max_length":     400,
    },
    {
        "description":    "Spanish language response",
        "intent":         "Proyectos en España",
        "language":       "es",
        "rows": [
            {"project_name": "Aurora",    "project_country": "Spain", "mwe": "150"},
            {"project_name": "Socovos",   "project_country": "Spain", "mwe": "90"},
        ],
        # At least one Spanish word should appear
        "must_mention":   ["proyecto", "españa", "aurora", "socovos", "mw"],
        "must_mention_any": True,
        "must_not_contain": ["{"],
        "min_length":     30,
        "max_length":     600,
    },
    {
        "description":    "Budget aggregation result",
        "intent":         "Total development budget per country for 2025",
        "language":       "en",
        "rows": [
            {"project_country": "Germany", "total_budget": "5200000"},
            {"project_country": "Spain",   "total_budget": "3800000"},
            {"project_country": "USA",     "total_budget": "9100000"},
        ],
        "must_mention":   ["germany", "spain", "usa"],
        "must_not_contain": ["{", "total_budget"],
        "min_length":     50,
        "max_length":     800,
    },
    {
        "description":    "Large result set — sample only sent to narrator",
        "intent":         "List all projects",
        "language":       "en",
        "rows":           [
            {"project_name": f"Project_{i}", "project_country": "Spain", "mwe": str(i*10)}
            for i in range(1, 51)   # 50 rows — narrator only gets sample of 5
        ],
        "must_mention":   ["project", "50", "spain"],
        "must_mention_any": True,
        "must_not_contain": ["{"],
        "min_length":     30,
        "max_length":     1000,
    },
]


def score_agent3_narrative(narrative: str, case: dict) -> Tuple[int, int, List[str]]:
    """Score narrative accuracy against expected criteria."""
    earned   = 0
    possible = 0
    notes    = []
    nl       = narrative.lower()

    # ── Length sanity (2 pts) ─────────────────────────────────────────────────
    possible += 2
    min_l = case.get("min_length", 20)
    max_l = case.get("max_length", 1000)
    if min_l <= len(narrative) <= max_l:
        earned += 2
        notes.append(f"✔ Length OK ({len(narrative)} chars, expected {min_l}–{max_l})")
    elif len(narrative) < min_l:
        notes.append(f"✘ Too short ({len(narrative)} chars, min {min_l})")
    else:
        earned += 1  # partial credit — at least something was generated
        notes.append(f"~ Too long ({len(narrative)} chars, max {max_l}) — partial credit")

    # ── Must-mention checks ───────────────────────────────────────────────────
    must_any = case.get("must_mention_any", False)
    mentions = case.get("must_mention", [])

    if must_any:
        # Only need ONE match
        possible += 3
        matched = [kw for kw in mentions if kw.lower() in nl]
        if matched:
            earned += 3
            notes.append(f"✔ Contains at least one required term: '{matched[0]}'")
        else:
            notes.append(f"✘ Contains NONE of required terms: {mentions}")
    else:
        # Need ALL matches
        for kw in mentions:
            possible += 1
            if kw.lower() in nl:
                earned += 1
                notes.append(f"✔ Mentions '{kw}'")
            else:
                notes.append(f"✘ MISSING mention of '{kw}'")

    # ── Must-not-contain checks (2 pts each — these are quality failures) ─────
    for bad_kw in case.get("must_not_contain", []):
        possible += 2
        if bad_kw.lower() not in nl:
            earned += 2
            notes.append(f"✔ No JSON leakage / raw key '{bad_kw}'")
        else:
            notes.append(f"✘ RAW DATA LEAKED into narrative: '{bad_kw}' found")

    return earned, possible, notes


def run_agent3_accuracy():
    hdr("AGENT 3 — Narrative Accuracy")

    try:
        from agents.narrator import format_and_narrate
    except ImportError as e:
        bad(f"Cannot import Agent 3: {e}")
        log("Agent3", "import", "IMPORT_ERROR", 0, 1, str(e))
        return 0

    total_earned   = 0
    total_possible = 0

    for i, case in enumerate(AGENT3_CASES, 1):
        sub(f"[{i}/{len(AGENT3_CASES)}] {case['description']} ({len(case['rows'])} rows, lang={case['language']})")

        result, err = safe_run(
            format_and_narrate,
            case["rows"],
            case["intent"],
            case["language"],
        )

        if err or result is None:
            bad(f"format_and_narrate() crashed: {err}")
            log("Agent3", case["description"], "CRASH", 0, 10, str(err))
            total_possible += 10
            continue

        # ── Structured data checks (3 pts) ────────────────────────────────────
        sd = result.get("structured_data", {})
        struct_earned = 0
        struct_possible = 3

        if sd.get("row_count") == len(case["rows"]):
            struct_earned += 1
            good(f"row_count correct: {sd.get('row_count')}")
        else:
            bad(f"row_count wrong: got {sd.get('row_count')}, expected {len(case['rows'])}")

        if isinstance(sd.get("columns"), list) and (not case["rows"] or len(sd["columns"]) == len(case["rows"][0])):
            struct_earned += 1
            good(f"columns correct: {sd.get('columns')}")
        else:
            bad(f"columns wrong: {sd.get('columns')}")

        sample_len = len(sd.get("sample", []))
        expected_sample = min(5, len(case["rows"]))
        if sample_len == expected_sample:
            struct_earned += 1
            good(f"sample length correct: {sample_len}")
        else:
            bad(f"sample length wrong: got {sample_len}, expected {expected_sample}")

        total_earned   += struct_earned
        total_possible += struct_possible
        log("Agent3", case["description"], "STRUCTURED_DATA",
            struct_earned, struct_possible, f"row_count={sd.get('row_count')}")

        # ── Narrative accuracy checks ─────────────────────────────────────────
        narrative = result.get("narrative", "")
        if not narrative:
            bad("narrative is empty")
            log("Agent3", case["description"], "NARRATIVE", 0, 10, "empty narrative")
            total_possible += 10
            continue

        earned, possible, notes = score_agent3_narrative(narrative, case)

        for n in notes:
            if n.startswith("✔"):  good(n[2:])
            elif n.startswith("✘"): bad(n[2:])
            else:                   note(n[2:])

        note(f"Narrative: \"{narrative[:120]}{'...' if len(narrative)>120 else ''}\"")

        total_earned   += earned
        total_possible += possible
        pct = round(100 * earned / possible) if possible else 0
        print(f"    {BOLD}Score: {earned}/{possible} ({pct}%){RESET}")
        log("Agent3", case["description"], "NARRATIVE", earned, possible, narrative[:80])

    overall_pct = round(100 * total_earned / total_possible) if total_possible else 0
    print(f"\n  {BOLD}Agent 3 Overall Accuracy: {total_earned}/{total_possible} = {overall_pct}%{RESET}")
    return overall_pct


# ══════════════════════════════════════════════════════════════════════════════
#  SAVE REPORT
# ══════════════════════════════════════════════════════════════════════════════

def save_report(scores: Dict[str, int]):
    ts   = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = f"eval_accuracy_report_{ts}.csv"
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["agent","question","dimension","score","max_score","pct","detail"])
        writer.writeheader()
        writer.writerows(_report_rows)
    print(f"\n  {BLUE}Report saved → {path}{RESET}")
    return path


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def print_final(scores: Dict[str, int]):
    print(f"\n{'═'*65}")
    print(f"{BOLD}  FINAL ACCURACY SCORES{RESET}")
    print(f"{'═'*65}")
    for agent, pct in scores.items():
        bar_len = pct // 5
        bar = "█" * bar_len + "░" * (20 - bar_len)
        colour = GREEN if pct >= 80 else (YELLOW if pct >= 60 else RED)
        print(f"  {BOLD}{agent:<10}{RESET}  {colour}{bar}{RESET}  {colour}{pct}%{RESET}")
    print(f"{'═'*65}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EPC accuracy evaluator")
    parser.add_argument("--agent", choices=["1","2","3"],
                        help="Evaluate only one agent. Evaluates all if omitted.")
    args = parser.parse_args()

    scores = {}

    if not args.agent or args.agent == "1":
        scores["Agent 1"] = run_agent1_accuracy()
    if not args.agent or args.agent == "2":
        scores["Agent 2"] = run_agent2_accuracy()
    if not args.agent or args.agent == "3":
        scores["Agent 3"] = run_agent3_accuracy()

    print_final(scores)
    save_report(scores)

    any_fail = any(v < 60 for v in scores.values())
    sys.exit(1 if any_fail else 0)
