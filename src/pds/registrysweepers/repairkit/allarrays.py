'''change single strings to array of strings'''

import logging

log = logging.getLogger(__name__)


def repair (document:{}, fieldname:str)->bool:
    log.debug(f'checking {fieldname}')
    if isinstance (document[fieldname], str):
        log.info (f'found string for {fieldname} where it should be an array')
        return {fieldname:[document[fieldname]]}
    return {}
