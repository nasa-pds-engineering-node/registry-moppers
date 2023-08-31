"""repairkit is an executable package

The reason repairkit is an executable package is for extension as new repairs
are needed in the future. They can be added by updating the REPAIR_TOOLS mapping
with the new field name and functional requirements. All the additions can then
be modules with this executable package.
"""
import logging
import re
from typing import Dict
from typing import Iterable
from typing import Union

from pds.registrysweepers.utils import configure_logging
from pds.registrysweepers.utils import query_registry_db
from pds.registrysweepers.utils.db.host import Host
from pds.registrysweepers.utils.db.update import Update

from . import allarrays
from ..utils.db import write_updated_docs

"""
dictionary repair tools is {field_name:[funcs]} where field_name can be:
  1: re.compile().fullmatch for the equivalent of "fred" == "fred"
  2: re.compile().match for more complex matching of subparts of the string

and funcs are:
def function_name (document:{}, fieldname:str)->{}

and the return an empty {} if no changes and {fieldname:new_value} for repairs

Examples

re.compile("^ops:Info/.+").match("ops:Info/ops:filesize")->match object
re.compile("^ops:Info/.+").fullmatch("ops:Info/ops:filesize")->match object
re.compile("^ops:Info/").match("ops:Info/ops:filesize")->match object
re.compile("^ops:Info/").fullmatch("ops:Info/ops:filesize")->None

To get str_a == str_b, re.compile(str_a).fullmatch

"""

REPAIR_TOOLS = {
    re.compile("^ops:Data_File_Info/").match: [allarrays.repair],
    re.compile("^ops:Label_File_Info/").match: [allarrays.repair],
}

log = logging.getLogger(__name__)


def generate_updates(docs: Iterable[Dict]) -> Iterable[Update]:
    """Lazily generate necessary Update objects for a collection of db documents"""
    for document in docs:
        id = document["_id"]
        src = document["_source"]
        repairs = {}
        log.debug(f"applying repairkit sweeper to document: {id}")
        for fieldname, data in src.items():
            for regex, funcs in REPAIR_TOOLS.items():
                if regex(fieldname):
                    for func in funcs:
                        repairs.update(func(src, fieldname))

        if repairs:
            log.info(f"Writing repairs to document: {id}")
            yield Update(id=id, content=repairs)


def run(
    base_url: str,
    username: str,
    password: str,
    verify_host_certs: bool = True,
    log_filepath: Union[str, None] = None,
    log_level: int = logging.INFO,
):
    configure_logging(filepath=log_filepath, log_level=log_level)
    log.info("Starting repairkit sweeper processing...")
    host = Host(password, base_url, username, verify_host_certs)

    all_docs = query_registry_db(host, {"match_all": {}}, {})
    updates = generate_updates(all_docs)
    write_updated_docs(host, updates)

    log.info("Repairkit sweeper processing complete!")
