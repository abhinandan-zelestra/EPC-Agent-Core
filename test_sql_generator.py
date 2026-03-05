import json
from typing import Dict, Any
from agents.sql_generator import generate_sql  # <-- CHANGE THIS
import csv

# -----------------------------
# Questions to Test
# -----------------------------

TEST_QUESTIONS = [
    "Give me development budget for germany from 2024-2026",
    "Give me pre ppa for spain for all years",
    "Give me all projects by country",
    "Give me the budget allocation per country for 2025",
    "Give me all projects by country segregated by stage",
    "Give me all projects by country segregated by status",
    "Give me a table for all projects by country segregated by status and stage",
    "Give me the NCF and year 1 P50 value for all projects in us",
    "Can you list all important financial assumptions for these projects (Total capex, total epc with breakdown, ebitda and all irrs)",
    "Can you give me the projects under carias",
    "Can you give me the projects handled by gavin berg",
    "Can you give among the germany projects which ones have achieved rfc and rff",
    "Can you tell what stage will the projects in germany be at end of dec 2025",
    "Now can you tell me the same by the end of 2026",
    "Can you compare the module equipment price among india and spain",
    "Compare the bess cost across all countries",
    "Give me the land lease requirements for aurora project",
    "Compare the grid connection details of aurora with wimke",
    "Can you give me the interconnection costs for all projects in india",
    "Can you compare the bop/bos cost between projects under miso",
    "Can you tell me which projects are ready for commercialisation in spain",
    "What will be the development progress for babilonia till 2026",
    "Which projects will finish construction in 2025 in spain",
    "Which projects will finish construction in 2026 in spain",
    "Give me the projects that will start construction in 2026 in spain",
    "What are the main drivers for a project having a significantly higher project IRR than others",
    "Give me the h1 budget for germany projects for 2025",
    "How much budget is required for Babilonia to change to next stage"
]


# -----------------------------
# Validation Logic
# -----------------------------

REQUIRED_KEYS = {
    "query_intent": str,
    "sql_query": str,
    "tables_used": list,
    "filters_applied": list,
    "business_rules_applied": list,
    "confidence_score": float,
}


def validate_response(response: Dict[str, Any]) -> str:
    if not isinstance(response, dict):
        return "Response is not a dict"

    if "error" in response:
        return f"Model returned error: {response['error']}"

    for key, expected_type in REQUIRED_KEYS.items():
        if key not in response:
            return f"Missing key: {key}"

        if not isinstance(response[key], expected_type):
            return f"Wrong type for {key}. Expected {expected_type}, got {type(response[key])}"

    if not response["sql_query"].strip():
        return "Empty SQL query"

    return "OK"


# -----------------------------
# Run Tests
# -----------------------------

def run_tests():
    total = len(TEST_QUESTIONS)
    passed = 0
    results = []

    for i, question in enumerate(TEST_QUESTIONS, 1):
        print("\n" + "=" * 80)
        print(f"TEST {i}/{total}")
        print("QUESTION:")
        print(question)
        print("-" * 80)

        try:
            result = generate_sql(question)
            validation = validate_response(result)

            if validation == "OK":
                status = "PASS"
                passed += 1
            else:
                status = "FAIL"

        except Exception as e:
            result = {"error": str(e)}
            status = "EXCEPTION"

        # Extract SQL if available
        sql_query = result.get("sql_query", "") if isinstance(result, dict) else ""

        # Save row
        results.append({
            "question": question,
            "status": status,
            "sql_query": sql_query,
            "response": json.dumps(result)
        })

    # Save to CSV
    with open("test_results.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["question", "status", "sql_query", "response"]
        )
        writer.writeheader()
        writer.writerows(results)

    print("\n" + "=" * 80)
    print(f"FINAL RESULT: {passed}/{total} PASSED")
    print("Results saved to test_results.csv")
    print("=" * 80)

if __name__ == "__main__":
    run_tests()