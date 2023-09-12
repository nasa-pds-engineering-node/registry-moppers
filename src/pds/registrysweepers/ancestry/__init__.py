import logging
from itertools import chain
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from typing import Union

from opensearchpy import OpenSearch
from pds.registrysweepers.ancestry.ancestryrecord import AncestryRecord
from pds.registrysweepers.ancestry.generation import get_bundle_ancestry_records
from pds.registrysweepers.ancestry.generation import get_collection_ancestry_records
from pds.registrysweepers.ancestry.generation import get_nonaggregate_ancestry_records
from pds.registrysweepers.utils import configure_logging
from pds.registrysweepers.utils import parse_args
from pds.registrysweepers.utils.db import write_updated_docs
from pds.registrysweepers.utils.db.client import get_opensearch_client
from pds.registrysweepers.utils.db.update import Update

log = logging.getLogger(__name__)

METADATA_PARENT_BUNDLE_KEY = "ops:Provenance/ops:parent_bundle_identifier"
METADATA_PARENT_COLLECTION_KEY = "ops:Provenance/ops:parent_collection_identifier"


def run(
    client: OpenSearch,
    log_filepath: Union[str, None] = None,
    log_level: int = logging.INFO,
    registry_mock_query_f: Optional[Callable[[str], Iterable[Dict]]] = None,
    ancestry_records_accumulator: Optional[List[AncestryRecord]] = None,
    bulk_updates_sink: Optional[List[Tuple[str, Dict[str, List]]]] = None,
):
    configure_logging(filepath=log_filepath, log_level=log_level)

    log.info("Starting ancestry sweeper")

    bundle_records = get_bundle_ancestry_records(client, registry_mock_query_f)
    collection_records = list(get_collection_ancestry_records(client, registry_mock_query_f))
    nonaggregate_records = get_nonaggregate_ancestry_records(client, collection_records, registry_mock_query_f)

    ancestry_records = chain(bundle_records, collection_records, nonaggregate_records)
    updates = generate_updates(ancestry_records, ancestry_records_accumulator, bulk_updates_sink)

    if bulk_updates_sink is None:
        log.info("Writing bulk updates to database...")
        write_updated_docs(client, updates)
    else:
        # consume generator to dump bulk updates to sink
        for _ in updates:
            pass

    log.info("Ancestry sweeper processing complete!")


def generate_updates(
    ancestry_records: Iterable[AncestryRecord], ancestry_records_accumulator=None, bulk_updates_sink=None
) -> Iterable[Update]:
    updates: Set[str] = set()

    log.info("Generating document bulk updates for AncestryRecords...")

    for record in ancestry_records:
        # Tee the stream of records into the accumulator, if one was provided (functional testing).
        if ancestry_records_accumulator is not None:
            ancestry_records_accumulator.append(record)

        if record.lidvid.is_collection() and len(record.parent_bundle_lidvids) == 0:
            log.warning(f"Collection {record.lidvid} is not referenced by any bundle.")

        doc_id = str(record.lidvid)
        update_content = {
            METADATA_PARENT_BUNDLE_KEY: [str(id) for id in record.parent_bundle_lidvids],
            METADATA_PARENT_COLLECTION_KEY: [str(id) for id in record.parent_collection_lidvids],
        }

        # Tee the stream of bulk update KVs into the accumulator, if one was provided (functional testing).
        if bulk_updates_sink is not None:
            bulk_updates_sink.append((doc_id, update_content))

        if doc_id in updates:
            log.error(
                f"Multiple updates detected for doc_id {doc_id} - cannot create update! (new content {update_content} will not be written)"
            )
            continue

        updates.add(doc_id)
        yield Update(id=doc_id, content=update_content)


if __name__ == "__main__":
    cli_description = f"""
    Update registry records for non-latest LIDVIDs with up-to-date direct ancestry metadata ({METADATA_PARENT_BUNDLE_KEY} and {METADATA_PARENT_COLLECTION_KEY}).

    Retrieves existing published LIDVIDs from the registry, determines membership identities for each LID, and writes updated docs back to registry db
    """

    args = parse_args(description=cli_description)
    client = get_opensearch_client(
        endpoint_url=args.base_URL, username=args.username, password=args.password, verify_certs=not args.insecure
    )

    run(
        client=client,
        log_level=args.log_level,
        log_filepath=args.log_file,
    )
