"""
Pipeline Integration Test
=========================
Tests Agent 1, Agent 2, and Agent 3 individually and then end-to-end.

HOW TO RUN:
    python3 test_pipeline.py             # run all tests
    python3 test_pipeline.py --agent 1   # test only Agent 1
    python3 test_pipeline.py --agent 2   # test only Agent 2
    python3 test_pipeline.py --agent 3   # test only Agent 3
    python3 test_pipeline.py --agent e2e # test full end-to-end pipeline

WHAT EACH SECTION TESTS:
    Agent 1  — Does it return valid JSON with all required keys?
               Is the SQL non-empty and syntactically plausible?
               Does confidence_score compute correctly?

    Agent 2  — Does it accept a known-good SQL (from Agent 1 output)?
               Does it REJECT SQL with fake tables / fake columns?
               Does it REJECT a low-confidence Agent 1 output?
               Does the dialect repair run without crashing?

    Agent 3  — Does it format rows correctly (stats, sample, row_count)?
               Does it return a non-empty narrative string?
               Does it handle an empty result set gracefully?
               Does it adapt language when asked?

    E2E      — Does the full pipeline return all expected keys?
               Does a question that should return data actually return rows?
               Does the downloadable_csv URL get generated?
               Does a bad question return a structured error (not a crash)?
"""

import sys
import json
import argparse
import traceback
from typing import Any, Dict

# ── colour helpers ────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
BLUE   = "\033[94m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def ok(msg):   print(f"  {GREEN}✅ PASS{RESET}  {msg}")
def fail(msg): print(f"  {RED}❌ FAIL{RESET}  {msg}")
def warn(msg): print(f"  {YELLOW}⚠️  WARN{RESET}  {msg}")
def info(msg): print(f"  {BLUE}ℹ️  INFO{RESET}  {msg}")
def header(msg): print(f"\n{BOLD}{BLUE}{'─'*60}{RESET}\n{BOLD}{msg}{RESET}\n{'─'*60}")

# ── result tracker ────────────────────────────────────────────────────────────
results = {"passed": 0, "failed": 0, "warned": 0}

def check(condition: bool, pass_msg: str, fail_msg: str, warning_only=False):
    if condition:
        ok(pass_msg)
        results["passed"] += 1
    elif warning_only:
        warn(fail_msg)
        results["warned"] += 1
    else:
        fail(fail_msg)
        results["failed"] += 1

def safe_run(fn, *args, **kwargs):
    """Run fn(*args) and return (result, error_string). Never raises."""
    try:
        return fn(*args, **kwargs), None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}\n{traceback.format_exc()}"


# ══════════════════════════════════════════════════════════════════════════════
# AGENT 1 TESTS
# ══════════════════════════════════════════════════════════════════════════════

def test_agent1():
    header("AGENT 1 — SQL Generator")

    from agents.sql_generator import generate_sql
    from services.glue_service import get_glue_schema
    from services.kb_service import retrieve_kb_context

    REQUIRED_KEYS = ["query_intent", "sql_query", "tables_used",
                     "filters_applied", "business_rules_applied", "confidence_score"]

    # Build real context (same as orchestration does)
    print("  Fetching schema and KB context...")
    kb = retrieve_kb_context("test")
    schema = get_glue_schema("zelestra-epc")
    context = {
        "schema": kb + "\n" + schema,
        "business_rules": "Exclude Abandoned/Sold projects by default."
    }

    test_questions = [
        ("Simple country filter",     "Give me all projects in Germany"),
        ("Aggregation",               "How many projects do we have per country?"),
        ("Budget query",              "Give me the development budget for Germany in 2025"),
        ("Financial assumptions",     "What is the total CAPEX for Spain projects?"),
        ("Multi-table join",          "List all pipeline projects in Spain with their IRR"),
    ]

    for label, question in test_questions:
        print(f"\n  [{label}] Q: {question[:70]}")
        result, err = safe_run(generate_sql, question, context=context)

        if err:
            fail(f"generate_sql() raised an exception:\n    {err}")
            results["failed"] += 1
            continue

        # Must not be an error response
        check("error" not in result,
              "No error key in response",
              f"Returned error: {result.get('error')} | {result.get('exception','')[:80]}")

        if "error" in result:
            continue

        # All required keys present
        missing = [k for k in REQUIRED_KEYS if k not in result]
        check(len(missing) == 0,
              f"All required keys present",
              f"Missing keys: {missing}")

        # SQL is non-empty and has SELECT + FROM
        sql = result.get("sql_query", "").lower()
        check("select" in sql and "from" in sql,
              f"SQL contains SELECT...FROM",
              f"SQL looks incomplete: {result.get('sql_query','')[:80]}")

        # Confidence score is numeric and in range
        conf = result.get("confidence_score", -1)
        check(isinstance(conf, float) and 0.0 <= conf <= 1.0,
              f"confidence_score in [0,1]: {conf}",
              f"confidence_score out of range or wrong type: {conf}")

        # tables_used is a non-empty list
        tables = result.get("tables_used", [])
        check(isinstance(tables, list) and len(tables) > 0,
              f"tables_used populated: {tables}",
              f"tables_used empty or wrong type",
              warning_only=True)

        info(f"Intent: {result.get('query_intent','?')[:60]}")
        info(f"SQL preview: {result.get('sql_query','')[:80].strip()}...")


# ══════════════════════════════════════════════════════════════════════════════
# AGENT 2 TESTS
# ══════════════════════════════════════════════════════════════════════════════

def test_agent2():
    header("AGENT 2 — SQL Validator")

    # Agent 2 and its dependencies
    try:
        from agents.sql_validator import validate_and_repair, CONFIDENCE_THRESHOLD
        from services.glue_service import get_glue_schema_dict
    except ImportError as e:
        fail(f"Cannot import Agent 2 — is sql_validator.py in agents/? Error: {e}")
        results["failed"] += 1
        return

    print("  Fetching Glue schema dict for whitelist checks...")
    schema_dict, err = safe_run(get_glue_schema_dict, "zelestra-epc")
    if err:
        fail(f"get_glue_schema_dict() failed: {err}")
        results["failed"] += 1
        return

    check(len(schema_dict) > 0,
          f"Glue schema_dict loaded ({len(schema_dict)} tables)",
          "Glue schema_dict is empty")

    # Pick the first table and one of its columns for a known-good test
    first_table = next(iter(schema_dict))
    first_col   = schema_dict[first_table][0] if schema_dict[first_table] else "record_id"
    info(f"Using table '{first_table}' col '{first_col}' for whitelist tests")

    # ── Test 1: known-good SQL passes ─────────────────────────────────────────
    print("\n  [Test 1] Known-good SQL should pass Stage 1")
    good_agent1 = {
        "sql_query":       f"SELECT {first_col} FROM {first_table} LIMIT 10",
        "confidence_score": 0.9,
        "query_intent":    "test",
        "tables_used":     [first_table],
        "filters_applied": [],
        "business_rules_applied": [],
    }
    result, err = safe_run(validate_and_repair, good_agent1, schema_dict)
    check(err is None and isinstance(result, str) and len(result) > 0,
          f"Valid SQL passed Agent 2 and returned repaired SQL",
          f"Valid SQL was rejected or errored: {err}")

    # ── Test 2: fake table is rejected ────────────────────────────────────────
    print("\n  [Test 2] SQL with fake table should be rejected")
    bad_table_agent1 = {
        "sql_query":        "SELECT id FROM totally_fake_table_xyz LIMIT 5",
        "confidence_score": 0.9,
        "query_intent":     "test",
        "tables_used":      ["totally_fake_table_xyz"],
        "filters_applied":  [],
        "business_rules_applied": [],
    }
    result, err = safe_run(validate_and_repair, bad_table_agent1, schema_dict)
    check(err is not None and "totally_fake_table_xyz" in err,
          "Fake table correctly rejected by whitelist check",
          f"Fake table was NOT rejected — this is a security gap. result={result}")

    # ── Test 3: low confidence is rejected ────────────────────────────────────
    print(f"\n  [Test 3] Confidence below {CONFIDENCE_THRESHOLD} should be rejected")
    low_conf_agent1 = {
        "sql_query":        f"SELECT {first_col} FROM {first_table}",
        "confidence_score": 0.1,
        "query_intent":     "test",
        "tables_used":      [first_table],
        "filters_applied":  [],
        "business_rules_applied": [],
    }
    result, err = safe_run(validate_and_repair, low_conf_agent1, schema_dict)
    check(err is not None and "confidence" in err.lower(),
          f"Low confidence ({0.1}) correctly rejected",
          f"Low confidence was NOT rejected. result={result}")

    # ── Test 4: empty SQL is rejected ─────────────────────────────────────────
    print("\n  [Test 4] Empty SQL should be rejected")
    empty_agent1 = {
        "sql_query":        "",
        "confidence_score": 0.9,
        "query_intent":     "test",
        "tables_used":      [],
        "filters_applied":  [],
        "business_rules_applied": [],
    }
    result, err = safe_run(validate_and_repair, empty_agent1, schema_dict)
    check(err is not None,
          "Empty SQL correctly rejected",
          f"Empty SQL was NOT rejected")

    # ── Test 5: dialect repair runs on a real question ────────────────────────
    print("\n  [Test 5] Full Agent 1 → Agent 2 chain on a real question")
    from agents.sql_generator import generate_sql
    from services.glue_service import get_glue_schema
    from services.kb_service import retrieve_kb_context

    kb     = retrieve_kb_context("projects in Germany")
    schema = get_glue_schema("zelestra-epc")
    context = {"schema": kb + "\n" + schema, "business_rules": "Exclude Abandoned/Sold."}

    agent1_out, err = safe_run(generate_sql, "Give me all projects in Germany", context=context)
    if err or "error" in (agent1_out or {}):
        warn(f"Agent 1 failed in chain test — skipping Agent 2 chain test")
        results["warned"] += 1
    else:
        repaired, err = safe_run(validate_and_repair, agent1_out, schema_dict)
        check(err is None and isinstance(repaired, str) and len(repaired) > 0,
              f"Agent 1 → Agent 2 chain produced repaired SQL",
              f"Agent 1 → Agent 2 chain failed: {err}")
        if repaired:
            info(f"Repaired SQL preview: {repaired[:80].strip()}...")


# ══════════════════════════════════════════════════════════════════════════════
# AGENT 3 TESTS
# ══════════════════════════════════════════════════════════════════════════════

def test_agent3():
    header("AGENT 3 — Formatter & Narrator")

    try:
        from agents.narrator import format_and_narrate
    except ImportError as e:
        fail(f"Cannot import Agent 3 — is narrator.py in agents/? Error: {e}")
        results["failed"] += 1
        return

    # ── Test 1: format with real data ─────────────────────────────────────────
    print("\n  [Test 1] Formatting a normal result set")
    sample_rows = [
        {"project_name": "Aurora",    "project_country": "Spain",   "stage": "AdvancedDevelopment", "mwe": "150"},
        {"project_name": "Babilonia", "project_country": "Germany", "stage": "LateDevelopment",      "mwe": "80"},
        {"project_name": "Klevenow",  "project_country": "Germany", "stage": "AdvancedDevelopment", "mwe": "200"},
        {"project_name": "Jasper",    "project_country": "USA",     "stage": "Sourcing",              "mwe": "300"},
        {"project_name": "Civita",    "project_country": "Italy",   "stage": "LateDevelopment",      "mwe": "120"},
        {"project_name": "Socovos",   "project_country": "Spain",   "stage": "AdvancedDevelopment", "mwe": "90"},
    ]
    result, err = safe_run(format_and_narrate, sample_rows, "List projects by country", "en")

    check(err is None,
          "format_and_narrate() ran without error",
          f"format_and_narrate() crashed: {err}")

    if result:
        sd = result.get("structured_data", {})

        check(sd.get("row_count") == 6,
              f"row_count correct (6)",
              f"row_count wrong: {sd.get('row_count')}")

        check(isinstance(sd.get("columns"), list) and len(sd["columns"]) == 4,
              f"columns correct: {sd.get('columns')}",
              f"columns wrong: {sd.get('columns')}")

        check(len(sd.get("sample", [])) == 5,
              "sample is 5 rows (capped at 5)",
              f"sample wrong length: {len(sd.get('sample', []))}")

        check("mwe" in sd.get("summary_stats", {}),
              "summary_stats computed for numeric column 'mwe'",
              "summary_stats missing for numeric column 'mwe'",
              warning_only=True)

        narrative = result.get("narrative", "")
        check(isinstance(narrative, str) and len(narrative) > 20,
              f"Narrative generated ({len(narrative)} chars)",
              f"Narrative too short or missing: '{narrative}'")

        info(f"Narrative: {narrative[:120]}...")

    # ── Test 2: empty result set ───────────────────────────────────────────────
    print("\n  [Test 2] Empty result set handled gracefully")
    result, err = safe_run(format_and_narrate, [], "Find projects in Antarctica", "en")
    check(err is None,
          "Empty rows handled without error",
          f"Empty rows caused a crash: {err}")
    if result:
        sd = result.get("structured_data", {})
        check(sd.get("row_count") == 0,
              "row_count is 0 for empty result",
              f"row_count wrong for empty: {sd.get('row_count')}")
        narrative = result.get("narrative", "")
        check(len(narrative) > 10,
              f"Narrative still generated for empty result ({len(narrative)} chars)",
              "No narrative for empty result")
        info(f"Empty narrative: {narrative[:100]}")

    # ── Test 3: language adaptation ───────────────────────────────────────────
    print("\n  [Test 3] Language adaptation (Spanish)")
    result_es, err = safe_run(format_and_narrate, sample_rows[:3], "Proyectos en España", "es")
    check(err is None,
          "Spanish language request ran without error",
          f"Spanish language request crashed: {err}")
    if result_es:
        narrative_es = result_es.get("narrative", "")
        check(len(narrative_es) > 20,
              f"Spanish narrative generated ({len(narrative_es)} chars)",
              "Spanish narrative too short")
        info(f"Spanish narrative: {narrative_es[:120]}...")


# ══════════════════════════════════════════════════════════════════════════════
# END-TO-END TESTS
# ══════════════════════════════════════════════════════════════════════════════

def test_e2e():
    header("END-TO-END — Full Pipeline")

    try:
        from orchestration import run_pipeline
    except ImportError as e:
        fail(f"Cannot import run_pipeline from orchestration.py: {e}")
        results["failed"] += 1
        return

    EXPECTED_KEYS = ["status", "query_intent", "executed_sql",
                     "narrative", "structured_data", "agent1_meta"]

    # ── Test 1: simple question returns all keys ───────────────────────────────
    print("\n  [Test 1] Simple question — check all output keys present")
    result, err = safe_run(run_pipeline, "How many projects are there per country?")

    check(err is None,
          "run_pipeline() completed without exception",
          f"run_pipeline() raised: {err}")

    if result:
        check(result.get("status") == "ok",
              f"Pipeline status is 'ok'",
              f"Pipeline status: {result.get('status')} | message: {result.get('message','')}")

        missing = [k for k in EXPECTED_KEYS if k not in result]
        check(len(missing) == 0,
              "All expected output keys present",
              f"Missing keys: {missing}")

        check(isinstance(result.get("executed_sql"), str) and len(result.get("executed_sql","")) > 10,
              "executed_sql is a non-empty string",
              f"executed_sql missing or empty")

        check(isinstance(result.get("narrative"), str) and len(result.get("narrative","")) > 20,
              f"narrative present ({len(result.get('narrative',''))} chars)",
              "narrative missing or too short")

        sd = result.get("structured_data", {})
        check(isinstance(sd.get("row_count"), int),
              f"structured_data.row_count is int: {sd.get('row_count')}",
              "structured_data.row_count missing or wrong type")

        meta = result.get("agent1_meta", {})
        check(isinstance(meta.get("confidence_score"), float),
              f"agent1_meta.confidence_score: {meta.get('confidence_score')}",
              "agent1_meta.confidence_score missing")

        csv_url = result.get("downloadable_csv")
        check(csv_url is None or (isinstance(csv_url, str) and csv_url.startswith("https://")),
              f"downloadable_csv is a valid URL or None (got: {str(csv_url)[:60]})",
              f"downloadable_csv has unexpected value: {csv_url}")

        info(f"Intent: {result.get('query_intent','?')[:70]}")
        info(f"Rows:   {sd.get('row_count', '?')}")
        info(f"SQL:    {result.get('executed_sql','')[:80].strip()}...")
        info(f"Story:  {result.get('narrative','')[:100]}...")

    # ── Test 2: question with data ─────────────────────────────────────────────
    print("\n  [Test 2] A question that should return actual data rows")
    result2, err2 = safe_run(run_pipeline, "Give me all projects in Germany")

    check(err2 is None,
          "Germany query ran without exception",
          f"Germany query raised: {err2}")

    if result2 and result2.get("status") == "ok":
        row_count = result2.get("structured_data", {}).get("row_count", 0)
        check(row_count > 0,
              f"Query returned {row_count} rows (data is flowing through)",
              f"Query returned 0 rows — Athena may have run but returned nothing",
              warning_only=True)
    elif result2:
        warn(f"Pipeline returned status={result2.get('status')} at stage={result2.get('stage','?')}: {result2.get('message','')}")
        results["warned"] += 1

    # ── Test 3: pipeline returns structured error, not a crash ────────────────
    print("\n  [Test 3] Nonsense question — should return structured error, not crash")
    result3, err3 = safe_run(run_pipeline, "xyzzy this is complete nonsense no table exists foobar")

    check(err3 is None,
          "Nonsense question did not raise an unhandled exception",
          f"Nonsense question caused an unhandled crash: {err3}")

    if result3:
        # Should either succeed (model tries its best) or return a structured error
        check("status" in result3,
              f"Response has 'status' key (status={result3.get('status')})",
              "Response has no 'status' key — error handling broken")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def print_summary():
    total = results["passed"] + results["failed"] + results["warned"]
    print(f"\n{'═'*60}")
    print(f"{BOLD}TEST SUMMARY{RESET}")
    print(f"{'═'*60}")
    print(f"  {GREEN}Passed : {results['passed']}{RESET}")
    print(f"  {RED}Failed : {results['failed']}{RESET}")
    print(f"  {YELLOW}Warned : {results['warned']}{RESET}")
    print(f"  Total  : {total}")
    print(f"{'═'*60}")
    if results["failed"] == 0:
        print(f"\n{GREEN}{BOLD}All checks passed.{RESET}\n")
    else:
        print(f"\n{RED}{BOLD}{results['failed']} check(s) failed — see above.{RESET}\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EPC Pipeline test runner")
    parser.add_argument("--agent", choices=["1", "2", "3", "e2e"],
                        help="Run only one section (1, 2, 3, or e2e). Runs all if omitted.")
    args = parser.parse_args()

    run_all = args.agent is None

    if run_all or args.agent == "1":
        test_agent1()
    if run_all or args.agent == "2":
        test_agent2()
    if run_all or args.agent == "3":
        test_agent3()
    if run_all or args.agent == "e2e":
        test_e2e()

    print_summary()
    sys.exit(0 if results["failed"] == 0 else 1)
