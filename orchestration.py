"""
Orchestration — Full Pipeline
==============================
Wires Agent 1 → Agent 2 → Athena → Agent 3 → S3 export.

  Stage 1  Agent 1  generate_sql()         LLM — intent + SQL generation
  Stage 2  Agent 2  validate_and_repair()  Code + LLM — whitelist check then dialect repair
  Stage 3  Athena   execute_query()        AWS Athena — runs the validated SQL
  Stage 4  Agent 3  format_and_narrate()   Code + LLM — structure rows + plain-English summary
  Stage 5  S3       export_to_s3()         AWS S3 — CSV upload + presigned URL (non-fatal)

Error at any stage returns:
  { "status": "error", "stage": "<stage_name>", "message": "<reason>" }
"""

import logging

from agents.sql_generator import generate_sql
from agents.sql_validator import validate_and_repair
from agents.narrator import format_and_narrate

from services.glue_service import get_glue_schema_text, get_glue_schema_dict
from services.kb_service import retrieve_kb_context
from services.athena_service import execute_query
from services.s3_export_service import export_to_s3

logger = logging.getLogger(__name__)

DATABASE = "zelestra-epc"

BUSINESS_RULES = """
GLOBAL POLICY RULES:
- By default exclude projects where status IN ('Abandoned','Sold') unless explicitly requested.
- Never fabricate missing values.
- If requested data does not exist in schema, generate SQL reflecting limitation.
- Always use COUNT(DISTINCT site_name) when counting projects.
""".strip()


def run_pipeline(question: str, language: str = "en") -> dict:
    """
    Run the full Agent 1 → 2 → Athena → Agent 3 → S3 pipeline.
    """

    logger.info(f"Pipeline started for question: {question}")

    # ── Fetch schema + KB context ───────────────────────────────────────────
    try:
        kb_context = retrieve_kb_context(question)
        schema_text = get_glue_schema_text(DATABASE)
        schema_dict = get_glue_schema_dict(DATABASE)

        logger.info(f"KB context length: {len(kb_context)} characters")
        logger.info(f"Schema tables loaded: {len(schema_dict)}")

    except Exception as e:
        logger.error(f"Schema/KB fetch failed: {str(e)}")
        return {"status": "error", "stage": "schema_fetch", "message": str(e)}

    # ── Stage 1: Agent 1 — SQL generation ───────────────────────────────────
    try:
        agent1 = generate_sql(
            question,
            context={
                "schema": schema_text,
                "knowledge_base": kb_context,
                "business_rules": BUSINESS_RULES
            }
        )

        if "error" in agent1:
            return {
                "status": "error",
                "stage": "agent1_sql_generation",
                "message": agent1.get("error", "Agent 1 failed"),
                "detail": agent1.get("exception", "")
            }

        logger.info("Agent 1 SQL generated")
        logger.info(agent1.get("sql_query"))

    except Exception as e:
        logger.error(f"Agent 1 failed: {str(e)}")
        return {"status": "error", "stage": "agent1_sql_generation", "message": str(e)}

    # ── Stage 2: Agent 2 — validate + repair ────────────────────────────────
    try:
        validated_sql = validate_and_repair(agent1, schema_dict)

        logger.info("Agent 2 validation passed")

    except ValueError as e:
        logger.warning(f"Agent 2 validation failed: {str(e)}")
        return {"status": "error", "stage": "agent2_validation", "message": str(e)}

    except Exception as e:
        logger.error(f"Agent 2 error: {str(e)}")
        return {"status": "error", "stage": "agent2_validation", "message": str(e)}

    # ── Stage 3: Athena execution ───────────────────────────────────────────
    try:
        rows = execute_query(validated_sql)

        logger.info(f"Athena returned {len(rows)} rows")

    except Exception as e:
        logger.error(f"Athena execution failed: {str(e)}")
        return {"status": "error", "stage": "athena_execution", "message": str(e)}

    # ── Stage 4: Agent 3 — narrate results ──────────────────────────────────
    try:
        agent3 = format_and_narrate(rows, agent1.get("query_intent", question), language)

    except Exception as e:
        logger.error(f"Agent 3 failed: {str(e)}")
        return {"status": "error", "stage": "agent3_narrator", "message": str(e)}

    # ── Stage 5: Export to S3 (non-fatal) ───────────────────────────────────
    csv_url = None
    if rows:
        try:
            csv_url = export_to_s3(rows)
        except Exception as e:
            logger.warning(f"S3 export failed (non-fatal): {e}")

    return {
        "status": "ok",
        "query_intent": agent1.get("query_intent", ""),
        "executed_sql": validated_sql,
        "narrative": agent3["narrative"],
        "structured_data": agent3["structured_data"],
        "downloadable_csv": csv_url,
        "agent1_meta": {
            "tables_used": agent1.get("tables_used", []),
            "filters_applied": agent1.get("filters_applied", []),
            "business_rules_applied": agent1.get("business_rules_applied", []),
            "confidence_score": agent1.get("confidence_score", 0.0),
        }
    }