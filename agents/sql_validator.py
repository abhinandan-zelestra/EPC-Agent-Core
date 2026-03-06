"""
Agent 2 — SQL Validator (Production Version)
=============================================

Responsibilities:
1. Confidence Gate
2. Self-healing SQL
3. Query safety guard (cost protection)
4. Glue schema whitelist validation
5. Athena dialect repair

Pipeline:

Agent 1 SQL
      │
      ▼
Confidence Check
      │
      ▼
Self-Healing SQL
      │
      ▼
Query Guard
      │
      ▼
Schema Whitelist Validation
      │
      ▼
Athena Dialect Repair
      │
      ▼
Validated SQL → Athena
"""

import json
import re
import logging
from typing import Set
from difflib import get_close_matches

import boto3

logger = logging.getLogger(__name__)

BEDROCK = boto3.client("bedrock-runtime")
MODEL_ID = "anthropic.claude-3-haiku-20240307-v1:0"

CONFIDENCE_THRESHOLD = 0.70
MAX_JOINS = 6
MAX_LIMIT = 10000


# --------------------------------------------------
# Main Entry
# --------------------------------------------------

def validate_and_repair(agent1_output: dict, glue_schema: dict) -> str:

    sql = agent1_output.get("sql_query", "").strip()

    if not sql:
        raise ValueError("Agent 1 returned empty SQL.")

    confidence = agent1_output.get("confidence_score", 0.0)

    if confidence < CONFIDENCE_THRESHOLD:
        raise ValueError(
            f"Agent 1 confidence too low ({confidence}). "
            "Query generation considered unreliable."
        )

    logger.info(f"Agent1 confidence: {confidence}")

    # Step 1 — self healing
    sql = _heal_sql(sql, glue_schema)

    # Step 2 — query safety guard
    sql = _guard_query(sql)

    # Step 3 — schema validation
    _whitelist_check(sql, glue_schema)

    # Step 4 — dialect repair
    sql = _repair_dialect(sql)

    logger.info("Agent 2 validation completed successfully")

    return sql


# --------------------------------------------------
# Self Healing SQL
# --------------------------------------------------

def _heal_sql(sql: str, glue_schema: dict) -> str:

    repaired = sql

    dotted_refs = re.findall(r'\b(\w+)\.(\w+)\b', sql)

    for table, column in dotted_refs:

        table_l = table.lower()

        if table_l not in glue_schema:
            continue

        allowed_cols = [c.lower() for c in glue_schema[table_l]]

        if column.lower() not in allowed_cols:

            suggestion = _suggest_column(column, allowed_cols)

            if suggestion:

                logger.info(
                    f"Self-healing column: {table}.{column} → {table}.{suggestion}"
                )

                repaired = re.sub(
                    rf"{table}\.{column}",
                    f"{table}.{suggestion}",
                    repaired,
                    flags=re.IGNORECASE
                )

    return repaired


def _suggest_column(column: str, allowed: list):

    matches = get_close_matches(
        column.lower(),
        allowed,
        n=1,
        cutoff=0.75
    )

    return matches[0] if matches else None


# --------------------------------------------------
# Query Guard
# --------------------------------------------------

def _guard_query(sql: str) -> str:

    sql_l = sql.lower()

    joins = sql_l.count(" join ")

    if joins > MAX_JOINS:
        raise ValueError(
            f"Query rejected: too many joins ({joins})"
        )

    if "cross join" in sql_l:
        raise ValueError("Query rejected: CROSS JOIN not allowed")

    if "limit" not in sql_l:
        sql += f"\nLIMIT {MAX_LIMIT}"

    return sql


# --------------------------------------------------
# Schema Whitelist Validation
# --------------------------------------------------

_SKIP_NAMES: Set[str] = {
    "select","from","where","and","or","not","in","like","between","is","null",
    "case","when","then","else","end","order","by","group","having","limit",
    "offset","union","all","distinct","as","on","join","inner","left","right",
    "outer","full","cross","exists","count","sum","avg","min","max","coalesce",
    "row_number","over","partition","current_date","current_timestamp"
}


def _whitelist_check(sql: str, glue_schema: dict):

    allowed_tables = {t.lower() for t in glue_schema}

    allowed_columns = {
        t.lower(): {c.lower() for c in cols}
        for t, cols in glue_schema.items()
    }

    sql_l = sql.lower()

    cte_names = _extract_cte_names(sql_l)

    violations = []

    for table in _extract_table_refs(sql_l):

        if table in cte_names:
            continue

        if table not in allowed_tables:
            violations.append(f"Unknown table: {table}")

    for qualifier, col in _extract_dotted_refs(sql_l):

        if qualifier in allowed_columns:

            if col not in allowed_columns[qualifier] and col != "*":
                violations.append(f"Unknown column: {qualifier}.{col}")

    if violations:

        raise ValueError(
            "Schema whitelist validation failed:\n"
            + "\n".join(violations)
        )


# --------------------------------------------------
# Regex Extractors
# --------------------------------------------------

def _extract_cte_names(sql: str):

    return set(
        re.findall(r'(?:with|,)\s+(\w+)\s+as\s*\(', sql)
    )


def _extract_table_refs(sql: str):

    refs = set()

    for m in re.finditer(r'(?:from|join)\s+([\w`"\[\]]+)', sql):

        name = m.group(1).strip('"`[]').lower()

        if name not in _SKIP_NAMES:
            refs.add(name)

    return refs


def _extract_dotted_refs(sql: str):

    refs = set()

    for m in re.finditer(r'\b(\w+)\.(\w+)\b', sql):

        qualifier = m.group(1).lower()
        col = m.group(2).lower()

        if qualifier.isdigit():
            continue

        refs.add((qualifier, col))

    return refs


# --------------------------------------------------
# Athena Dialect Repair
# --------------------------------------------------

DIALECT_PROMPT = """
You are an Athena SQL dialect repair engine.

Your job is ONLY to fix SQL syntax issues.

Rules:
- Do NOT change table names
- Do NOT change column names
- Do NOT change logic
- Only fix dialect syntax

Return JSON:

{
"repaired_sql": "",
"changes_made": []
}
"""


def _repair_dialect(sql: str):

    body = {

        "anthropic_version": "bedrock-2023-05-31",

        "system": DIALECT_PROMPT,

        "messages": [
            {
                "role": "user",
                "content": f"Fix Athena SQL dialect issues:\n\n{sql}"
            }
        ],

        "max_tokens": 1000,
        "temperature": 0
    }

    try:

        response = BEDROCK.invoke_model(
            modelId=MODEL_ID,
            body=json.dumps(body)
        )

        raw = json.loads(response["body"].read())

        text = re.sub(
            r"```json|```",
            "",
            raw["content"][0]["text"]
        ).strip()

        result = json.loads(text)

        repaired = result.get("repaired_sql", "").strip()

        if repaired:
            return repaired

    except Exception as e:

        logger.warning(f"Dialect repair failed: {e}")

    return sql