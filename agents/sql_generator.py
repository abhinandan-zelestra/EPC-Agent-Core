import boto3
import json
import re
import logging

bedrock = boto3.client("bedrock-runtime")

MODEL_ID = "anthropic.claude-3-haiku-20240307-v1:0"

logger = logging.getLogger(__name__)


def compute_confidence(parsed: dict) -> float:
    score = 1.0

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

    if "select" not in sql or "from" not in sql:
        score -= 0.3

    if not parsed.get("tables_used"):
        score -= 0.1

    if not parsed.get("filters_applied"):
        score -= 0.1

    return max(round(score, 3), 0.0)


def generate_sql(question: str, context: dict = None):

    schema = context.get("schema", "") if context else ""
    kb_context = context.get("knowledge_base", "") if context else ""
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
- Only use tables listed in the Glue schema section.
- Athena DB preset is 'zelestra-epc'. Do NOT prefix table names.
"""

    user_prompt = f"""
User Question:
{question}

Business Context (Knowledge Base):
{kb_context}

Glue Schema:
{schema}

Business Rules:
{business_rules}
"""

    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "system": system_prompt,
        "messages": [
            {"role": "user", "content": user_prompt}
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

    logger.info("Agent 1 SQL generated")
    logger.debug(output_text)

    try:
        parsed = _robust_parse(output_text)

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

    cleaned = re.sub(r"```json|```", "", text, flags=re.MULTILINE).strip()

    try:
        result = json.loads(cleaned)
        if isinstance(result, dict) and "sql_query" in result:
            return result
    except:
        pass

    match = re.search(r'\{.*\}', cleaned, re.DOTALL)
    if match:
        return json.loads(match.group())

    raise ValueError("Failed to parse model output")