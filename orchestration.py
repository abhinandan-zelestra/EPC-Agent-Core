from agents.sql_generator import generate_sql
from services.glue_service import get_glue_schema
from services.kb_service import retrieve_kb_context


def run_pipeline(question: str):

    # 1️⃣ Get KB context (business logic + schema descriptions)
    kb_context = retrieve_kb_context(question)

    # 2️⃣ Get live Glue schema
    glue_schema = get_glue_schema("zelestra-epc")

    # 3️⃣ Combine everything
    combined_schema = kb_context + "\n" + glue_schema

    business_rules = """
    GLOBAL POLICY RULES:

- By default exclude projects where status IN ('Abandoned','Sold') unless explicitly requested.
- Never fabricate missing values.
- If requested data does not exist in schema, generate SQL reflecting limitation.
- Always use COUNT(DISTINCT site_name) when counting projects.
    """

    # 4️⃣ Call SQL Generator
    sql_output = generate_sql(
        question,
        context={
            "schema": combined_schema,
            "business_rules": business_rules
        }
    )

    return sql_output