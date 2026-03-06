import re

MAX_JOINS = 6
MAX_LIMIT = 10000


def guard_query(sql: str):

    sql_l = sql.lower()

    # join protection
    joins = sql_l.count(" join ")

    if joins > MAX_JOINS:
        raise ValueError(
            f"Query too complex: {joins} joins detected"
        )

    # cross join
    if "cross join" in sql_l:
        raise ValueError("CROSS JOIN not allowed")

    # enforce LIMIT
    if "limit" not in sql_l:
        sql += f"\nLIMIT {MAX_LIMIT}"

    return sql