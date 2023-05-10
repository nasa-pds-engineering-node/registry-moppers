import logging
from dataclasses import dataclass
from dataclasses import field
from enum import auto
from enum import Enum
from itertools import chain
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Set
from typing import Union

from pds.registrysweepers.utils import configure_logging
from pds.registrysweepers.utils import HOST
from pds.registrysweepers.utils import parse_args
from pds.registrysweepers.utils import query_registry_db
from pds.registrysweepers.utils import write_updated_docs
from pds.registrysweepers.utils.productidentifiers.factory import PdsProductIdentifierFactory
from pds.registrysweepers.utils.productidentifiers.pdslid import PdsLid
from pds.registrysweepers.utils.productidentifiers.pdslidvid import PdsLidVid

log = logging.getLogger("registrysweepers.ancestry")

METADATA_PARENT_BUNDLE_KEY = "ops:Provenance/ops:parent_bundle_identifiers"
METADATA_PARENT_COLLECTION_KEY = "ops:Provenance/ops:parent_collection_identifiers"


@dataclass
class AncestryRecord:
    lidvid: PdsLidVid
    parent_collection_lidvids: List[PdsLidVid] = field(default_factory=list)
    parent_bundle_lidvids: List[PdsLidVid] = field(default_factory=list)

    def __repr__(self):
        return f"AncestryRecord(lidvid={self.lidvid}, parent_collection_lidvids={[str(x) for x in self.parent_collection_lidvids]}, parent_bundle_lidvids={[str(x) for x in self.parent_bundle_lidvids]})"

    def __hash__(self):
        return self.lidvid.__hash__()


class ProductClass(Enum):
    BUNDLE = (auto(),)
    COLLECTION = (auto(),)
    NON_AGGREGATE = auto()


def product_class_query_factory(cls: ProductClass) -> Dict:
    queries: Dict[ProductClass, Dict] = {
        ProductClass.BUNDLE: {"bool": {"filter": [{"term": {"product_class": "Product_Bundle"}}]}},
        ProductClass.COLLECTION: {"bool": {"filter": [{"term": {"product_class": "Product_Collection"}}]}},
        ProductClass.NON_AGGREGATE: {
            "bool": {"must_not": [{"terms": {"product_class": ["Product_Bundle", "Product_Collection"]}}]}
        },
    }

    return queries[cls]


def get_bundle_ancestry_records(host: HOST) -> Iterable[AncestryRecord]:
    query = product_class_query_factory(ProductClass.BUNDLE)
    _source = {"includes": ["lidvid"]}

    results = query_registry_db(host, query, _source)

    return [AncestryRecord(lidvid=PdsLidVid.from_string(doc["_source"]["lidvid"])) for doc in results]


def get_collection_ancestry_records(host: HOST) -> Iterable[AncestryRecord]:
    # Query the registry for all bundles and the collections each references.
    top_down_ref_search_query = product_class_query_factory(ProductClass.BUNDLE)
    top_down_ref_search_source = {"includes": ["lidvid", "ref_lid_collection"]}
    top_down_ref_search_docs = query_registry_db(host, top_down_ref_search_query, top_down_ref_search_source)

    # Query the registry for all collection identifiers
    collection_identifiers_query = product_class_query_factory(ProductClass.COLLECTION)
    collection_identifiers_source = {"includes": ["lidvid"]}
    # TODO: switch to this line to support alternate_ids
    # collection_identifiers_source = {"includes": ["alternate_ids"]}  # alternate_ids includes lid, lidvid, and any aliases
    collection_identifiers_docs = query_registry_db(host, collection_identifiers_query, collection_identifiers_source)

    # TODO: change to for loop to support alternate_ids (generate multiple keys per element and filter out LIDs)
    #  also necessary to populate a multidirectional lookup for LIDs with aliases which can be used during LID-based
    #  reference assignment
    # Instantiate the AncestryRecords, keyed by collection LIDVID for fast access
    ancestry_by_collection_lidvid: Dict[PdsLidVid, AncestryRecord] = {
        PdsLidVid.from_string(doc["_source"]["lidvid"]): AncestryRecord(
            lidvid=PdsLidVid.from_string(doc["_source"]["lidvid"])
        )
        for doc in collection_identifiers_docs
    }

    # Create a dict of pointer-sets to the newly-instantiated records, binned/keyed by LID for fast access when a bundle
    #  only refers to a LID rather than a specific LIDVID
    ancestry_by_collection_lid: Dict[PdsLid, Set[AncestryRecord]] = {}
    for record in ancestry_by_collection_lidvid.values():
        if record.lidvid.lid not in ancestry_by_collection_lid:
            ancestry_by_collection_lid[record.lidvid.lid] = set()
        ancestry_by_collection_lid[record.lidvid.lid].add(record)

    # For each bundle, add it to the bundle-ancestry of every collection it references
    for doc in top_down_ref_search_docs:
        bundle_lidvid = PdsLidVid.from_string(doc["_source"]["lidvid"])
        referenced_collection_identifiers = [
            PdsProductIdentifierFactory.from_string(id) for id in doc["_source"]["ref_lid_collection"]
        ]

        for identifier in referenced_collection_identifiers:
            if identifier.__class__ is PdsLidVid:
                try:
                    # if a LIDVID is specified, add bundle to that LIDVID's record
                    ancestry_by_collection_lidvid[identifier].parent_bundle_lidvids.append(bundle_lidvid)
                except KeyError:
                    log.warning(
                        f"Collection {identifier} referenced by bundle {bundle_lidvid} does not exist in registry - skipping"
                    )
            elif identifier.__class__ is PdsLid:
                try:
                    # else if a LID is specified, add bundle to the record of every LIDVID with that LID
                    for record in ancestry_by_collection_lid[identifier]:
                        record.parent_bundle_lidvids.append(bundle_lidvid)
                except KeyError:
                    log.warning(
                        f"No versions of collection {identifier} referenced by bundle {bundle_lidvid} exist in registry - skipping"
                    )
            else:
                raise RuntimeError(f"Encountered unknown PdsProductIdentifier subclass {identifier.__class__}")

    # We could retain the keys for better performance, as they're used by the non-aggregate record generation, but this
    # is cleaner, so we'll regenerate the dict from the records later unless performance is a problem.
    return ancestry_by_collection_lidvid.values()


def get_nonaggregate_ancestry_records(host: HOST, collection_ancestry_records: Iterable[AncestryRecord]):
    # Generate lookup for the parent bundles of all collections - these will be applied to non-aggregate products too.
    bundle_ancestry_by_collection_lidvid = {
        record.lidvid: record.parent_bundle_lidvids for record in collection_ancestry_records
    }

    # Query the registry-refs index for the contents of all collections
    top_down_ref_search_query = {"match_all": {}}  # type: ignore
    top_down_ref_search_source = {"includes": ["collection_lidvid", "product_lidvid"]}
    top_down_ref_search_docs = query_registry_db(
        host, top_down_ref_search_query, top_down_ref_search_source, index_name="registry-refs"
    )

    nonaggregate_ancestry_records_by_lidvid = {}

    # For each collection, add the collection and its bundle ancestry to all products the collection contains
    for doc in top_down_ref_search_docs:
        collection_lidvid = PdsLidVid.from_string(doc["_source"]["collection_lidvid"])
        bundle_ancestry = bundle_ancestry_by_collection_lidvid[collection_lidvid]

        nonaggregate_lidvids = [PdsLidVid.from_string(s) for s in doc["_source"]["product_lidvid"]]
        for lidvid in nonaggregate_lidvids:
            if lidvid not in nonaggregate_ancestry_records_by_lidvid:
                nonaggregate_ancestry_records_by_lidvid[lidvid] = AncestryRecord(lidvid=lidvid)

            record = nonaggregate_ancestry_records_by_lidvid[lidvid]
            record.parent_bundle_lidvids.extend(bundle_ancestry)
            record.parent_collection_lidvids.append(collection_lidvid)

    return nonaggregate_ancestry_records_by_lidvid.values()


def run(
    base_url: str,
    username: str,
    password: str,
    cross_cluster_remotes=None,
    verify_host_certs: bool = False,
    log_filepath: Union[str, None] = None,
    log_level: int = logging.INFO,
):
    configure_logging(filepath=log_filepath, log_level=log_level)

    log.info("starting ancestry sweeper processing")

    host = HOST(cross_cluster_remotes or [], password, base_url, username, verify_host_certs)

    bundle_records = get_bundle_ancestry_records(host)
    collection_records = list(
        get_collection_ancestry_records(host)
    )  # list cast avoids consumption of the iterable as it is used later
    nonaggregate_records = get_nonaggregate_ancestry_records(host, collection_records)

    ancestry_records = chain(bundle_records, collection_records, nonaggregate_records)

    updates: Dict[str, Dict[str, Any]] = {}
    for record in ancestry_records:
        if record.lidvid.is_collection() and len(record.parent_bundle_lidvids) == 0:
            log.warning(f"Collection {record.lidvid} is not referenced by any bundle.")

        doc_id = str(record.lidvid)
        update = {
            METADATA_PARENT_BUNDLE_KEY: [str(id) for id in record.parent_bundle_lidvids],
            METADATA_PARENT_COLLECTION_KEY: [str(id) for id in record.parent_collection_lidvids],
        }
        if doc_id in updates:
            existing_update = updates[doc_id]
            log.error(
                f"Multiple updates detected for doc_id {doc_id} - cannot create update! (got {update}, {existing_update} already exists)"
            )
            continue

        updates[doc_id] = update

    if updates:
        write_updated_docs(host, updates)

    # TODO: Search for and log any orphaned non-agg products - it's reasonably sufficient to just look for any product which lacks the relevant metadata keys.

    log.info("completed ancestry sweeper processing")


if __name__ == "__main__":
    cli_description = f"""
    Update registry records for non-latest LIDVIDs with up-to-date direct ancestry metadata ({METADATA_PARENT_BUNDLE_KEY} and {METADATA_PARENT_COLLECTION_KEY}).

    Retrieves existing published LIDVIDs from the registry, determines membership identities for each LID, and writes updated docs back to OpenSearch
    """

    args = parse_args(description=cli_description)

    run(
        base_url=args.base_URL,
        username=args.username,
        password=args.password,
        cross_cluster_remotes=args.cluster_nodes,
        verify_host_certs=args.verify,
        log_level=args.log_level,
        log_filepath=args.log_file,
    )
