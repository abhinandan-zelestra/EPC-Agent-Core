import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def log_query(question, sql, tables):

    entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "question": question,
        "sql": sql,
        "tables": tables
    }

    logger.info(json.dumps(entry))