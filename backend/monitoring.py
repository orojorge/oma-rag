import json
import logging

_log = logging.getLogger("oma.monitoring")


def emit_query_log(data: dict) -> None:
    _log.info(json.dumps({"event": "query", **data}))
