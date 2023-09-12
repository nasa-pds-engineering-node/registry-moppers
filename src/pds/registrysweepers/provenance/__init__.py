#! /usr/bin/env python3
# Copyright © 2023, California Institute of Technology ("Caltech").
# U.S. Government sponsorship acknowledged.
#
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# • Redistributions of source code must retain the above copyright notice,
#   this list of conditions and the following disclaimer.
# • Redistributions must reproduce the above copyright notice, this list of
#   conditions and the following disclaimer in the documentation and/or other
#   materials provided with the distribution.
# • Neither the name of Caltech nor its operating division, the Jet Propulsion
#   Laboratory, nor the names of its contributors may be used to endorse or
#   promote products derived from this software without specific prior written
#   permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#
# provenance
# ==========
#
# Determines if a particular document has been superseded by a more
# recent version, if upon which it has, sets the field
# ops:Provenance/ops:superseded_by to the id of the superseding document.
#
# It is important to note that the document is updated, not any dependent
# index.
#
import logging
from typing import Dict
from typing import Iterable
from typing import List
from typing import Mapping
from typing import Union

from opensearchpy import OpenSearch
from pds.registrysweepers.utils import _vid_as_tuple_of_int
from pds.registrysweepers.utils import configure_logging
from pds.registrysweepers.utils import parse_args
from pds.registrysweepers.utils.db import get_extant_lidvids
from pds.registrysweepers.utils.db import write_updated_docs
from pds.registrysweepers.utils.db.client import get_opensearch_client
from pds.registrysweepers.utils.db.update import Update

log = logging.getLogger(__name__)

METADATA_SUCCESSOR_KEY = "ops:Provenance/ops:superseded_by"


def run(
    client: OpenSearch,
    log_filepath: Union[str, None] = None,
    log_level: int = logging.INFO,
):
    configure_logging(filepath=log_filepath, log_level=log_level)

    log.info("Starting provenance sweeper processing...")

    # host = Host(password, base_url, username, verify_host_certs)

    extant_lidvids = get_extant_lidvids(client)
    successors = get_successors_by_lidvid(extant_lidvids)
    updates = generate_updates(successors)

    write_updated_docs(client, updates)

    log.info("Completed provenance sweeper processing!")


def get_successors_by_lidvid(extant_lidvids: Iterable[str]) -> Mapping[str, str]:
    """
    Given a collection of LIDVIDs, return a new mapping to their updated direct successors.
    """

    log.info("Generating updated history...")

    extant_lidvids = list(extant_lidvids)  # ensure against consumable iterator

    unique_lids = {lidvid.split("::")[0] for lidvid in extant_lidvids}

    log.info("   ...binning LIDVIDs by LID...")
    lidvid_aggregates_by_lid: Dict[str, List[str]] = {lid: [] for lid in unique_lids}
    for lidvid in extant_lidvids:
        lid = lidvid.split("::")[0]
        lidvid_aggregates_by_lid[lid].append(lidvid)

    log.info("   ...determining updated successors for LIDVIDs...")
    successors_by_lidvid = {}
    lidvid_aggregates_with_multiple_versions = filter(lambda l: 1 < len(l), lidvid_aggregates_by_lid.values())
    for lidvids in lidvid_aggregates_with_multiple_versions:
        lidvids.sort(key=_vid_as_tuple_of_int, reverse=True)

        for successor_idx, lidvid in enumerate(lidvids[1:]):
            # TODO: consider converting this dict accumulation to a tuple/dict generator (yield) for memory optimization
            successors_by_lidvid[lidvid] = lidvids[successor_idx]

    log.info(f"Successors will be updated for {len(successors_by_lidvid)} LIDVIDs!")

    if log.isEnabledFor(logging.DEBUG):
        for lidvid in successors_by_lidvid.keys():
            log.debug(f"{lidvid}")

    return successors_by_lidvid


def generate_updates(successors_by_id: Mapping[str, str]) -> Iterable[Update]:
    for id, successor in successors_by_id.items():
        update_content = {METADATA_SUCCESSOR_KEY: successor}
        yield Update(id=id, content=update_content)


if __name__ == "__main__":
    cli_description = f"""
    Update registry records for non-latest LIDVIDs with up-to-date direct successor metadata ({METADATA_SUCCESSOR_KEY}).

    Retrieves existing published LIDVIDs from the registry, determines history for each LID, and writes updated docs back to registry db.
    """

    cli_epilog = """EXAMPLES:

    - command for opensearch running in a container with the sockets published at 9200 for data ingested for full day March 11, 2020:

      registrysweepers.py -b https://localhost:9200 -p admin -u admin

    - getting more help on availables arguments and what is expected:

      registrysweepers.py --help

    """

    args = parse_args(description=cli_description, epilog=cli_epilog)
    client = get_opensearch_client(
        endpoint_url=args.base_URL, username=args.username, password=args.password, verify_certs=not args.insecure
    )

    run(
        client=client,
        log_level=args.log_level,
        log_filepath=args.log_file,
    )
