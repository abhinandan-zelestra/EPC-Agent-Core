"""
Agent 3 — Narrator
==================
Two responsibilities:

  format_rows()      — Structures raw Athena rows into a clean dict with
                        row_count, columns, summary_stats (numeric cols),
                        and a 5-row sample. This is deterministic, no LLM.

  narrate()          — Sends a trimmed payload (sample only, not full rows)
                        to Claude to write a plain-English summary.
                        Full rows are excluded to prevent context overflow
                        on large result sets.

  format_and_narrate() — Convenience wrapper that calls both in sequence.
"""

import json
import re
import logging
from typing import Any, Dict, List

import boto3

logger = logging.getLogger(__name__)

BEDROCK  = boto3.client("bedrock-runtime")
MODEL_ID = "anthropic.claude-3-haiku-20240307-v1:0"

NARRATOR_SYSTEM = """
You are a business data analyst who writes clear, concise summaries of query results.

RULES:
- Write in plain English. No jargon, no technical column names.
- Lead with the single most important finding.
- Be factual — only state what the data shows.
- Keep the response to 2–5 sentences for small result sets, up to 8 for large ones.
- Do NOT mention JSON, SQL, row counts as meta-information unless directly relevant.
- Do NOT include raw column names (e.g. project_country, budget_year_f).
- If the result set is empty, clearly state that no data matched the query.
- Adapt to the language specified in the request.
""".strip()


# ── Public API ────────────────────────────────────────────────────────────────

def format_and_narrate(rows: List[Dict], intent: str, language: str = "en") -> Dict:
    """
    Format raw Athena rows and generate a narrative summary.

    Args:
        rows:     List of dicts from athena_service.execute_query().
        intent:   The query_intent string from Agent 1 (used as context).
        language: 'en', 'es', 'de', etc. Narrator adapts accordingly.

    Returns:
        {
            "structured_data": { row_count, columns, summary_stats, sample, rows },
            "narrative": "<plain-English summary>"
        }
    """
    structured = _format_rows(rows)
    narrative  = _narrate(structured, intent, language)
    return {
        "structured_data": structured,
        "narrative": narrative,
    }


# ── Formatter ─────────────────────────────────────────────────────────────────

def _format_rows(rows: List[Dict]) -> Dict:
    if not rows:
        return {
            "row_count":     0,
            "columns":       [],
            "summary_stats": {},
            "sample":        [],
            "rows":          [],
        }

    columns = list(rows[0].keys())
    sample  = rows[:5]

    # Compute summary stats for numeric columns
    stats = {}
    for col in columns:
        values = []
        for row in rows:
            try:
                values.append(float(row[col]))
            except (TypeError, ValueError):
                pass
        if values:
            stats[col] = {
                "min":   round(min(values), 4),
                "max":   round(max(values), 4),
                "sum":   round(sum(values), 4),
                "avg":   round(sum(values) / len(values), 4),
                "count": len(values),
            }

    return {
        "row_count":     len(rows),
        "columns":       columns,
        "summary_stats": stats,
        "sample":        sample,
        "rows":          rows,
    }


# ── Narrator ──────────────────────────────────────────────────────────────────

def _narrate(structured: Dict, intent: str, language: str) -> str:
    # Send trimmed payload — full rows excluded to avoid context overflow
    payload = {
        "row_count":     structured["row_count"],
        "columns":       structured["columns"],
        "summary_stats": structured["summary_stats"],
        "sample":        structured["sample"],
    }

    lang_instruction = (
        f"Respond in {language}." if language != "en"
        else "Respond in English."
    )

    user_prompt = (
        f"Query intent: {intent}\n\n"
        f"Data:\n{json.dumps(payload, indent=2)}\n\n"
        f"{lang_instruction} Write a clear summary of what this data shows."
    )

    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "system": NARRATOR_SYSTEM,
        "messages": [{"role": "user", "content": user_prompt}],
        "max_tokens": 512,
        "temperature": 0,
    }

    response = BEDROCK.invoke_model(modelId=MODEL_ID, body=json.dumps(body))
    raw      = json.loads(response["body"].read())
    narrative = raw["content"][0]["text"].strip()

    logger.info(f"Agent 3 narrative generated ({len(narrative)} chars).")
    return narrative
