"""change single strings to array of strings"""
import logging
from typing import Dict

log = logging.getLogger(__name__)


def repair(document: Dict, fieldname: str) -> Dict:
    log.debug(f"checking {fieldname}")
    if isinstance(document[fieldname], str):
        log.debug(f"found string for {fieldname} where it should be an array")
        return {fieldname: [document[fieldname]]}
    return {}
