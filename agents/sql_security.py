FORBIDDEN_KEYWORDS = [
    "drop",
    "delete",
    "update",
    "insert",
    "truncate",
    "alter",
    "create",
]


def security_check(sql: str):
    sql_lower = sql.lower()

    for keyword in FORBIDDEN_KEYWORDS:
        if keyword in sql_lower:
            raise ValueError(f"Dangerous SQL detected: {keyword}")

    return True