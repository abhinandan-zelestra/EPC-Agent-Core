import boto3
import json
import os
import re

bedrock = boto3.client("bedrock-runtime")

MODEL_ID = "anthropic.claude-3-haiku-20240307-v1:0"

def compute_confidence(parsed: dict) -> float:
    score = 1.0

    # Required structure
    required_keys = [
        "query_intent",
        "sql_query",
        "tables_used",
        "filters_applied",
        "business_rules_applied"
    ]

    for key in required_keys:
        if key not in parsed:
            score -= 0.2

    sql = parsed.get("sql_query", "").lower()

    # Basic SQL validation
    if "select" not in sql or "from" not in sql:
        score -= 0.3

    if not parsed.get("tables_used"):
        score -= 0.1

    if not parsed.get("filters_applied"):
        score -= 0.1

    return max(round(score, 3), 0.0)

def generate_sql(question: str, context: dict = None):

    schema = context.get("schema", "") if context else ""
    business_rules = context.get("business_rules", "") if context else ""

    system_prompt = """
You are a deterministic Athena SQL generation engine.

Your only task is to generate correct SQL queries.

STRICT OUTPUT RULES:
- Output ONLY valid JSON.
- No markdown.
- No explanations.
- No comments.
- Return exactly one JSON object.
- Never fabricate tables or columns.
- Use exact column names from provided schema.
- Athena DB preset is 'zelestra-epc'. Do NOT prefix table names.

────────────────────────────
CORE SQL CONSTRUCTION RULES
────────────────────────────

1. Case Handling:
- Always use LOWER() for text comparisons.

2. Join & Deduplication:
- When joins are involved, use COUNT(DISTINCT project_id or record_id) to avoid double counting.
- When joining multiple one-to-many tables, use separate CTEs.
- Never aggregate across multiple JOINs simultaneously.
- Prefer IN or EXISTS to prevent fan-out duplication.

3. Region Normalization:
- LATAM / Latin America = Ecuador, Peru, Chile, Colombia.
- South Europe / Southern Europe = Italy, Spain, Portugal.
- If region column exists → filter region.
- Else → filter project_country using IN(...).
- BU refers to region.

4. Project Definitions:
- Portfolio projects:
  project_status IN ('Developing','Construction','Operating','Extended COD')
  AND stage IN ('Identified Opportunity','AdvancedDevelopment','LateDevelopment')

- Pipeline projects:
  project_status IN ('Developing','Extended COD')
  AND stage IN ('Identified Opportunity','AdvancedDevelopment','LateDevelopment')

- If Sourcing included → expand stage filter to include 'Sourcing'.

5. Canonical Naming:
- Stage string: 'AdvancedDevelopment'.
- Document stage: 'Corporate Valuation'.
- Activity name: 'PPA signed'.
- Do not confuse 'mwe' with 'total_capacity_mw'.

6. Latest Row / Versioned Table Logic:

For budget_details:
- Timestamp: date_modified
- Partition by: related_project, project_versioning_budget_label, budget_year_f, month_index

For project_assumptions:
- Timestamp: document_uploaded_date
- Partition by: site_name, document_stage, project_assumptions_clean, group

Template:
WITH latest AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY <partition_keys>
           ORDER BY <timestamp_column> DESC
         ) AS rn
  FROM <table_name>
)
SELECT ...
FROM latest
WHERE rn = 1

7. Budget SQL Rules:
- Budgets come from budget_details.
- Join using projects.record_id = budget_details.related_project.
- Always use deduplicated latest rows.
- Use budget_year_f for yearly filters.
- H1 = month_index IN (1-6)
- H2 = month_index IN (7-12)
- Do NOT exclude negative values.
- Include Abandoned projects unless explicitly filtered.

8. Financial Assumptions:
- Use project_assumptions_clean column only.
- Always filter by site_name and document_stage.
- Always select latest document_uploaded_date.
- Totals: Total Investment = Total CAPEX + Other + (Fees+Interests).
- OPEX → group='OPEX'
- CAPEX → group='CAPEX'
- Total CAPEX = Total EPC + Others CAPEX Costs.
- Total EPC = WTG + Modules + Inverters + Racking + BESS + BOP/BOS + Interconnection + Other Construction Costs.

9. Stage Advancement:
- Stages: Sourcing → Identified Opportunity → AdvancedDevelopment → LateDevelopment.
- Advancement only if all required activities for current stage AND country are completed.
- Do NOT mix countries or stages.
- RFC flag from projects.rfc_flag.
- RFF flag from projects.rff_flag.
- Development progress from projects.development_progress.

10. Construction Filters:
- Started construction:
  project_status IN ('Operating','Construction') AND NTP date filter.
- Finished construction:
  Filter by COD date.
- Future construction includes 'Developing'.

11. MWₑ Rule:
- Use stored column projects.mwe.
- Do NOT recalculate manually.

12. Strict Data Rule:
- Only use project-specific data from schema.
- Do not infer missing values.
- If required data is not present, generate SQL reflecting limitation.

────────────────────────────
RETURN FORMAT (MANDATORY)
────────────────────────────

{
  "query_intent": "",
  "sql_query": "",
  "tables_used": [],
  "filters_applied": [],
  "business_rules_applied": [],
  "confidence_score": 0.0
}
"""

    user_prompt = f"""
User Question:
{question}

Schema:
{schema}

Business Rules:
{business_rules}
"""

    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "system": system_prompt,
        "messages": [
            {
                "role": "user",
                "content": user_prompt
            }
        ],
        "max_tokens": 2000,
        "temperature": 0
    }

    response = bedrock.invoke_model(
        modelId=MODEL_ID,
        body=json.dumps(body)
    )

    raw = json.loads(response["body"].read())

    output_text = raw["content"][0]["text"]

    try:
        parsed = _robust_parse(output_text)

        # Overwrite model confidence with real computed score
        try:
            parsed["confidence_score"] = compute_confidence(parsed)
        except Exception:
            parsed["confidence_score"] = 0.0

        return parsed

    except Exception as e:
        return {
            "error": "Model did not return valid JSON",
            "exception": str(e),
            "confidence_score": 0.0,
            "raw_output": output_text
        }


def _robust_parse(text: str) -> dict:
    """
    Parse the model output into a dict, handling three known failure modes:

    1. Raw newlines inside JSON string values (most common).
       Claude writes sql_query as a human-readable multiline string with
       literal line breaks, which is invalid JSON.

    2. Double-encoded JSON (less common).
       The entire JSON object is wrapped in an extra layer of escaping —
       all quotes become \" and internal newlines appear as literal \n
       characters inside what looks like a string value.

    3. Markdown fences wrapping the JSON.

    Strategy: try each approach in order, return the first that gives a dict
    with the expected keys.
    """
    # Strip markdown fences
    cleaned = re.sub(r"```json|```", "", text, flags=re.MULTILINE).strip()

    # Attempt 1: plain parse (works when model output is already valid JSON)
    try:
        result = json.loads(cleaned)
        if isinstance(result, dict) and "sql_query" in result:
            return result
    except (json.JSONDecodeError, ValueError):
        pass

    # Attempt 2: escape raw newlines inside string values, then parse
    try:
        escaped = _escape_newlines_in_strings(cleaned)
        result = json.loads(escaped)
        if isinstance(result, dict) and "sql_query" in result:
            return result
    except (json.JSONDecodeError, ValueError):
        pass

    # Attempt 3: double-encoded — the model wrapped the JSON in an extra
    # escape layer. Replace \" with " and literal \n sequences with newlines,
    # then try again with newline escaping.
    try:
        # Strip outer quotes if the whole thing is wrapped in them
        unwrapped = cleaned
        if unwrapped.startswith('"') and unwrapped.endswith('"'):
            unwrapped = unwrapped[1:-1]
        # Unescape the double-encoding
        unwrapped = unwrapped.replace('\\"', '"').replace('\\n', '\n').replace('\\t', '\t')
        escaped = _escape_newlines_in_strings(unwrapped)
        result = json.loads(escaped)
        if isinstance(result, dict) and "sql_query" in result:
            return result
    except (json.JSONDecodeError, ValueError):
        pass

    # Attempt 4: regex extraction — grab the first {...} block and try parsing it
    match = re.search(r'\{.*\}', cleaned, re.DOTALL)
    if match:
        try:
            candidate = _escape_newlines_in_strings(match.group())
            result = json.loads(candidate)
            if isinstance(result, dict) and "sql_query" in result:
                return result
        except (json.JSONDecodeError, ValueError):
            pass

    raise ValueError(f"All parse attempts failed. Raw text (first 200 chars): {text[:200]}")


def _escape_newlines_in_strings(text: str) -> str:
    """
    Escape raw newlines and tabs that appear inside JSON string values.

    Walks the text character by character tracking whether we are inside a
    quoted string.  Only newlines/tabs found inside quotes are escaped —
    structural whitespace between JSON keys is left untouched.
    """
    result = []
    in_string = False
    escape_next = False

    for ch in text:
        if escape_next:
            result.append(ch)
            escape_next = False
            continue

        if ch == "\\" and in_string:
            result.append(ch)
            escape_next = True
            continue

        if ch == '"':
            in_string = not in_string
            result.append(ch)
            continue

        if in_string:
            if ch == "\n":
                result.append("\\n")
            elif ch == "\r":
                result.append("\\r")
            elif ch == "\t":
                result.append("\\t")
            else:
                result.append(ch)
        else:
            result.append(ch)

    return "".join(result)