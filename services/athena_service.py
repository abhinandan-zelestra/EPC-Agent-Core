"""
Athena Service
==============
Executes a validated SQL string against the zelestra-epc database
and returns results as a list of dicts.
"""

import boto3
import time
import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

athena = boto3.client("athena", region_name="eu-central-1")

DATABASE        = "zelestra-epc"
ATHENA_OUTPUT   = "s3://aws-athena-query-results-eu-central-1-891377001420/"
WORKGROUP       = "primary"
POLL_INTERVAL_S = 1.5
MAX_WAIT_S      = 120


def execute_query(sql: str) -> List[Dict[str, Any]]:
    """
    Execute SQL on Athena and return rows as [ { col: value, ... }, ... ].
    Raises RuntimeError on query failure or timeout.
    """
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        WorkGroup=WORKGROUP,
    )
    execution_id = response["QueryExecutionId"]
    logger.info(f"Athena query started: {execution_id}")

    # Poll until complete
    elapsed = 0.0
    while elapsed < MAX_WAIT_S:
        status_resp = athena.get_query_execution(QueryExecutionId=execution_id)
        state = status_resp["QueryExecution"]["Status"]["State"]

        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            reason = status_resp["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
            raise RuntimeError(f"Athena query {state}: {reason}")

        time.sleep(POLL_INTERVAL_S)
        elapsed += POLL_INTERVAL_S
    else:
        raise RuntimeError(f"Athena query timed out after {MAX_WAIT_S}s.")

    return _fetch_results(execution_id)


def _fetch_results(execution_id: str) -> List[Dict[str, Any]]:
    rows = []
    paginator = athena.get_paginator("get_query_results")
    pages = paginator.paginate(QueryExecutionId=execution_id)

    headers = None
    for page in pages:
        result_rows = page["ResultSet"]["Rows"]
        if headers is None:
            headers = [col["VarCharValue"] for col in result_rows[0]["Data"]]
            result_rows = result_rows[1:]   # skip header row
        for row in result_rows:
            values = [cell.get("VarCharValue", "") for cell in row["Data"]]
            rows.append(dict(zip(headers, values)))

    logger.info(f"Athena returned {len(rows)} row(s).")
    return rows
