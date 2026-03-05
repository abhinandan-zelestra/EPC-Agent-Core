"""
S3 Export Service
=================
Builds a CSV from result rows in-memory and uploads to S3.
Returns a presigned URL (1 hour TTL) for the user to download.

In-memory build avoids any /tmp filesystem dependency in the container.
"""

import io
import csv
import uuid
import boto3
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

s3 = boto3.client("s3", region_name="eu-central-1")

S3_BUCKET = "zelestra-epc-exports"
S3_PREFIX = "exports/"
URL_TTL_S = 3600   # 1 hour


def export_to_s3(rows: List[Dict[str, Any]], filename: Optional[str] = None) -> str:
    """
    Upload rows as CSV to S3 and return a presigned download URL.

    Args:
        rows:     List of dicts (same format returned by athena_service).
        filename: Optional S3 key suffix. Auto-generated if not provided.

    Returns:
        Presigned HTTPS URL valid for URL_TTL_S seconds.
    """
    if not rows:
        raise ValueError("Cannot export empty result set.")

    # Build CSV in memory
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    csv_bytes = buffer.getvalue().encode("utf-8")

    # Generate S3 key
    if not filename:
        ts  = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        uid = uuid.uuid4().hex[:8]
        filename = f"{ts}_{uid}.csv"

    key = S3_PREFIX + filename

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=csv_bytes,
        ContentType="text/csv",
    )
    logger.info(f"Uploaded {len(rows)} rows to s3://{S3_BUCKET}/{key}")

    url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": S3_BUCKET, "Key": key},
        ExpiresIn=URL_TTL_S,
    )
    return url
