import re
import logging
from difflib import get_close_matches

logger = logging.getLogger(__name__)


def heal_sql(sql: str, glue_schema: dict):

    repaired = sql

    dotted_refs = re.findall(r'\b(\w+)\.(\w+)\b', sql)

    for table, column in dotted_refs:

        table_l = table.lower()

        if table_l not in glue_schema:
            continue

        allowed_cols = glue_schema[table_l]

        if column.lower() not in allowed_cols:

            suggestion = _suggest(column, allowed_cols)

            if suggestion:

                logger.info(
                    f"Self healing {table}.{column} -> {table}.{suggestion}"
                )

                repaired = re.sub(
                    rf"{table}\.{column}",
                    f"{table}.{suggestion}",
                    repaired,
                    flags=re.IGNORECASE
                )

    return repaired


def _suggest(col, allowed):

    matches = get_close_matches(
        col,
        allowed,
        n=1,
        cutoff=0.75
    )

    return matches[0] if matches else None