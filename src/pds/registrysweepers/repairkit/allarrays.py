"""change single strings to array of strings"""
import json
import logging
from typing import Dict

log = logging.getLogger(__name__)

# exclude the following properties from array conversion even if targeted - they are expected to be string-typed
EXCLUDED_PROPERTIES = {"lid", "vid", "lidvid", "title", "product_class", "_package_id", "ops:Tracking_Meta/ops:archive_status"}


def repair(document: Dict, fieldname: str) -> Dict:
    # don't touch the enumerated exclusions, or any registry-sweepers metadata property
    if fieldname in EXCLUDED_PROPERTIES or fieldname.startswith("ops:Provenance"):
        return {}

    log.debug(f"checking {fieldname}")
    if isinstance(document[fieldname], str):
        log.debug(f"found string for {fieldname} where it should be an array")
        return {fieldname: [document[fieldname]]}
    return {}


# TODO: remove me once applied to prod -- edunn 20231206
def apply_reversion_fix(document: Dict, fieldname: str) -> Dict:
    src_val = document[fieldname]
    if isinstance(src_val, list) and len(src_val) == 1:
        return {fieldname: src_val[0]}
    else:
        log.error(
            f'Unexpected situation when applying reversion fix: Expected single-element array, got {src_val}, when targeting "{fieldname}" in {json.dumps(document)}'
        )
    return {}
