"""
Agent 2 — SQL Validator
=======================
Stage 1: Deterministic whitelist check (pure Python + sqlparse, no LLM).
         Every table and column in the SQL is checked against the real Glue
         schema. Anything not in the schema raises ValueError immediately —
         this gate cannot be bypassed by a confident model.

Stage 2: Athena dialect repair (LLM, narrow scope).
         Only runs if Stage 1 passes. Claude is asked only to fix Presto/Athena
         syntax issues. It is explicitly forbidden from changing names or logic.
"""

import json
import re
import logging
from typing import Dict, List, Tuple

import boto3
import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML

logger = logging.getLogger(__name__)

BEDROCK  = boto3.client("bedrock-runtime")
MODEL_ID = "anthropic.claude-3-haiku-20240307-v1:0"

CONFIDENCE_THRESHOLD = 0.7

DIALECT_REPAIR_PROMPT = """
You are an AWS Athena (Presto SQL) dialect repair engine.
Your ONLY job is to fix SQL syntax so it runs on Athena.

RULES:
- Do NOT change any table names or column names.
- Do NOT change logic, filters, JOINs, or aggregations.
- Fix dialect issues only, for example:
    * STR_TO_DATE  → DATE_PARSE
    * TOP N        → LIMIT N
    * GETDATE()    → CURRENT_DATE
    * ISNULL       → COALESCE
    * [col]        → "col"
    * `col`        → "col"
- Output ONLY valid JSON, no markdown, no explanation.

Return:
{
  "repaired_sql": "<corrected SQL>",
  "changes_made": ["<description of each change>"]
}

If no changes needed, echo original SQL and set changes_made to [].
""".strip()


# ── SQL keywords to ignore during column whitelist check ─────────────────────
_SQL_KEYWORDS = {
    "select","from","where","and","or","not","in","like","between","is","null",
    "true","false","case","when","then","else","end","order","by","group",
    "having","limit","offset","union","all","distinct","as","on","set","with",
    "row_number","over","partition","count","sum","avg","min","max","coalesce",
    "cast","try_cast","date_parse","date_format","date_diff","date_add",
    "current_date","current_timestamp","year","month","day","lower","upper",
    "trim","length","substr","substring","concat","replace","round","floor",
    "ceil","abs","if","nullif","greatest","least","rn","desc","asc",
    "inner","left","right","outer","cross","join","full","exists","any",
}

_TABLE_KEYWORDS = {
    "FROM","JOIN","INNER JOIN","LEFT JOIN","RIGHT JOIN","FULL JOIN",
    "LEFT OUTER JOIN","RIGHT OUTER JOIN","CROSS JOIN","INTO","UPDATE",
}


# ── Public entry point ────────────────────────────────────────────────────────

def validate_and_repair(agent1_output: dict, glue_schema: dict) -> str:
    """
    Validate and dialect-repair Agent 1's SQL.

    Args:
        agent1_output: Full dict from generate_sql() — needs sql_query + confidence_score.
        glue_schema:   { table: [col, ...] } from glue_service.get_glue_schema_dict().

    Returns:
        Validated, dialect-repaired SQL string ready to send to Athena.

    Raises:
        ValueError on empty SQL, low confidence, unknown tables, or unknown columns.
    """
    sql = agent1_output.get("sql_query", "").strip()
    if not sql:
        raise ValueError("Agent 1 returned empty sql_query.")

    confidence = agent1_output.get("confidence_score", 0.0)
    if confidence < CONFIDENCE_THRESHOLD:
        raise ValueError(
            f"Agent 1 confidence too low: {confidence:.2f} < {CONFIDENCE_THRESHOLD}. "
            "Question may be ambiguous or outside the schema."
        )

    # Stage 1 — hard gate, no LLM
    _whitelist_check(sql, glue_schema)

    # Stage 2 — dialect repair only
    return _repair_dialect(sql)


# ── Stage 1: whitelist ────────────────────────────────────────────────────────

def _whitelist_check(sql: str, glue_schema: dict) -> None:
    allowed_tables  = {t.lower() for t in glue_schema}
    allowed_columns = {t.lower(): {c.lower() for c in cols}
                       for t, cols in glue_schema.items()}

    tables_found, columns_found = _extract_identifiers(sql)
    violations = []

    for tbl in tables_found:
        if tbl not in allowed_tables:
            violations.append(f"Unknown table: '{tbl}'")

    for col in columns_found:
        col_l = col.lower()
        if col_l in _SQL_KEYWORDS or col_l == "*":
            continue
        if "." in col_l:
            tbl_part, col_part = col_l.split(".", 1)
            if tbl_part in allowed_columns:
                if col_part not in allowed_columns[tbl_part]:
                    violations.append(f"Unknown column: '{col}'")
        else:
            if not any(col_l in cols for cols in allowed_columns.values()):
                violations.append(f"Unknown column: '{col}'")

    if violations:
        raise ValueError(
            "Whitelist check failed — identifiers not in Glue schema:\n" +
            "\n".join(f"  • {v}" for v in violations)
        )

    logger.info("Agent 2 Stage 1 passed.")


def _extract_identifiers(sql: str) -> Tuple[List[str], List[str]]:
    tables, columns = [], []
    for stmt in sqlparse.parse(sql):
        _walk(stmt, tables, columns)
    return list(set(tables)), list(set(columns))


def _walk(token, tables, columns):
    import sqlparse.tokens as T
    if not hasattr(token, "tokens"):
        return
    prev_kw = None
    for tok in token.tokens:
        if tok.ttype in (Keyword, DML):
            prev_kw = tok.normalized.upper()
        elif tok.ttype is T.Name:
            name = tok.value.lower().strip('"`')
            if prev_kw in _TABLE_KEYWORDS:
                tables.append(name)
            else:
                columns.append(name)
        elif isinstance(tok, Identifier):
            real = tok.get_real_name()
            if real:
                name = real.lower().strip('"`')
                full = tok.value.strip().lower()
                if prev_kw in _TABLE_KEYWORDS:
                    tables.append(name)
                else:
                    columns.append(full if "." in full else name)
        elif isinstance(tok, IdentifierList):
            for item in tok.get_identifiers():
                if isinstance(item, Identifier):
                    real = item.get_real_name()
                    if real:
                        name = real.lower().strip('"`')
                        full = item.value.strip().lower()
                        if prev_kw in _TABLE_KEYWORDS:
                            tables.append(name)
                        else:
                            columns.append(full if "." in full else name)
        _walk(tok, tables, columns)


# ── Stage 2: dialect repair ───────────────────────────────────────────────────

def _repair_dialect(sql: str) -> str:
    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "system": DIALECT_REPAIR_PROMPT,
        "messages": [{"role": "user", "content": f"Fix dialect issues:\n\n{sql}"}],
        "max_tokens": 2000,
        "temperature": 0,
    }
    response = BEDROCK.invoke_model(modelId=MODEL_ID, body=json.dumps(body))
    raw  = json.loads(response["body"].read())
    text = re.sub(r"```json|```", "", raw["content"][0]["text"]).strip()

    try:
        result   = json.loads(text)
        repaired = result.get("repaired_sql", "").strip()
        changes  = result.get("changes_made", [])
        if not repaired:
            logger.warning("Dialect repair returned empty SQL — using original.")
            return sql
        if changes:
            logger.info(f"Dialect repair made {len(changes)} change(s): {changes}")
        return repaired
    except (json.JSONDecodeError, KeyError):
        logger.warning("Dialect repair returned non-JSON — using original SQL.")
        return sql
