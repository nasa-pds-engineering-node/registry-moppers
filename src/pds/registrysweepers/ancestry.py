import logging
from dataclasses import dataclass
from dataclasses import field
from enum import auto
from enum import Enum
from itertools import chain
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from typing import Union

from pds.registrysweepers.utils import configure_logging
from pds.registrysweepers.utils import HOST
from pds.registrysweepers.utils import parse_args
from pds.registrysweepers.utils import query_registry_db_or_mock
from pds.registrysweepers.utils import write_updated_docs
from pds.registrysweepers.utils.productidentifiers.factory import PdsProductIdentifierFactory
from pds.registrysweepers.utils.productidentifiers.pdslid import PdsLid
from pds.registrysweepers.utils.productidentifiers.pdslidvid import PdsLidVid

log = logging.getLogger(__name__)

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
        return hash(self.lidvid)


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


def get_bundle_ancestry_records(
    host: HOST, registry_db_mock: Optional[Callable[[str], Iterable[Dict]]] = None
) -> Iterable[AncestryRecord]:
    query = product_class_query_factory(ProductClass.BUNDLE)
    _source = {"includes": ["lidvid"]}
    query_f = query_registry_db_or_mock(registry_db_mock, "get_bundle_ancestry_records")
    docs = query_f(host, query, _source)  # type: ignore

    return [AncestryRecord(lidvid=PdsLidVid.from_string(doc["_source"]["lidvid"])) for doc in docs]


def get_collection_ancestry_records(
    host: HOST, registry_db_mock: Optional[Callable[[str], Iterable[Dict]]] = None
) -> Iterable[AncestryRecord]:
    # Query the registry for all bundles and the collections each references.
    bundles_query = product_class_query_factory(ProductClass.BUNDLE)
    bundles_source = {"includes": ["lidvid", "ref_lid_collection"]}
    bundles_query_f = query_registry_db_or_mock(registry_db_mock, "get_collection_ancestry_records_bundles")
    bundles_docs = bundles_query_f(host, bundles_query, bundles_source)  # type: ignore

    # Query the registry for all collection identifiers
    collections_query = product_class_query_factory(ProductClass.COLLECTION)
    collections_source = {"includes": ["lidvid", "alternate_ids"]}
    collections_query_f = query_registry_db_or_mock(registry_db_mock, "get_collection_ancestry_records_collections")

    # TODO: change to for loop to support alternate_ids (generate multiple keys per element and filter out LIDs)
    #  also necessary to populate a multidirectional lookup for LIDs with aliases which can be used during LID-based
    #  reference assignment
    # Instantiate the AncestryRecords, keyed by collection LIDVID for fast access
    ancestry_by_collection_lidvid = {}
    for doc in collections_query_f(host, collections_query, collections_source):
        lidvid = PdsLidVid.from_string(doc["_source"]["lidvid"])
        ancestry_by_collection_lidvid[lidvid] = AncestryRecord(lidvid=PdsLidVid.from_string(doc["_source"]["lidvid"]))

    collection_aliases_by_lid: Dict[PdsLid, Set[PdsLid]] = {}
    for doc in collections_query_f(host, collections_query, collections_source):
        alternate_ids: List[str] = doc["_source"].get("alternate_ids", [])
        lids: Set[PdsLid] = {PdsProductIdentifierFactory.from_string(id).lid for id in alternate_ids}
        for lid in lids:
            if lid not in collection_aliases_by_lid:
                collection_aliases_by_lid[lid] = set()
            collection_aliases_by_lid[lid].update(lids)

    # Create a dict of pointer-sets to the newly-instantiated records, binned/keyed by LID for fast access when a bundle
    #  only refers to a LID rather than a specific LIDVID
    ancestry_by_collection_lid: Dict[PdsLid, Set[AncestryRecord]] = {}
    for record in ancestry_by_collection_lidvid.values():
        if record.lidvid.lid not in ancestry_by_collection_lid:
            ancestry_by_collection_lid[record.lidvid.lid] = set()
        ancestry_by_collection_lid[record.lidvid.lid].add(record)

    # For each bundle, add it to the bundle-ancestry of every collection it references
    for doc in bundles_docs:
        bundle_lidvid = PdsLidVid.from_string(doc["_source"]["lidvid"])
        referenced_collection_identifiers = [
            PdsProductIdentifierFactory.from_string(id) for id in doc["_source"]["ref_lid_collection"]
        ]

        for identifier in referenced_collection_identifiers:
            # The following is janky from an OOP perspective, but this is a special case in that PdsLidVid and PdsLid
            # are and always will be complementary sets (given the universal set PdsProductIdentifier)
            if isinstance(identifier, PdsLidVid):
                try:
                    # if a LIDVID is specified, add bundle to that LIDVID's record
                    ancestry_by_collection_lidvid[identifier].parent_bundle_lidvids.append(bundle_lidvid)
                except KeyError:
                    log.warning(
                        f"Collection {identifier} referenced by bundle {bundle_lidvid} does not exist in registry - skipping"
                    )
            elif isinstance(identifier, PdsLid):
                try:
                    for alias in collection_aliases_by_lid[identifier]:
                        # else if a LID is specified, add bundle to the record of every LIDVID with that LID
                        for record in ancestry_by_collection_lid[alias]:
                            # TODO: make parent_* members sets to avoid need for this manual deduplication
                            record.parent_bundle_lidvids.append(bundle_lidvid)
                            record.parent_collection_lidvids = list(set(record.parent_collection_lidvids))
                except KeyError:
                    log.warning(
                        f"No versions of collection {identifier} referenced by bundle {bundle_lidvid} exist in registry - skipping"
                    )
            else:
                raise RuntimeError(
                    f"Encountered product identifier of unknown type {identifier.__class__} (should be PdsLidVid or PdsLid)"
                )

    # We could retain the keys for better performance, as they're used by the non-aggregate record generation, but this
    # is cleaner, so we'll regenerate the dict from the records later unless performance is a problem.
    return ancestry_by_collection_lidvid.values()


def get_nonaggregate_ancestry_records(
    host: HOST,
    collection_ancestry_records: Iterable[AncestryRecord],
    registry_db_mock: Optional[Callable[[str], Iterable[Dict]]] = None,
) -> Iterable[AncestryRecord]:
    # Generate lookup for the parent bundles of all collections - these will be applied to non-aggregate products too.
    bundle_ancestry_by_collection_lidvid = {
        record.lidvid: record.parent_bundle_lidvids for record in collection_ancestry_records
    }

    # Query the registry-refs index for the contents of all collections
    collection_refs_query = {"match_all": {}}  # type: ignore
    collection_refs_source = {"includes": ["collection_lidvid", "product_lidvid"]}
    collection_refs_query_f = query_registry_db_or_mock(registry_db_mock, "get_nonaggregate_ancestry_records")
    collection_refs_query_docs = collection_refs_query_f(
        host, collection_refs_query, collection_refs_source, index_name="registry-refs"
    )  # type: ignore

    nonaggregate_ancestry_records_by_lidvid = {}

    # For each collection, add the collection and its bundle ancestry to all products the collection contains
    for doc in collection_refs_query_docs:
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
    registry_mock_query_f: Optional[Callable[[str], Iterable[Dict]]] = None,
    ancestry_records_accumulator: Optional[List[AncestryRecord]] = None,
    bulk_updates_sink: Optional[List[Tuple[str, Dict[str, List]]]] = None,
):
    # TODO: Add informational logging to stages
    configure_logging(filepath=log_filepath, log_level=log_level)

    log.info("starting ancestry sweeper processing")

    host = HOST(cross_cluster_remotes or [], password, base_url, username, verify_host_certs)

    bundle_records = get_bundle_ancestry_records(host, registry_mock_query_f)
    # list cast avoids consumption of the iterable as it is used later
    collection_records = list(get_collection_ancestry_records(host, registry_mock_query_f))
    nonaggregate_records = get_nonaggregate_ancestry_records(host, collection_records, registry_mock_query_f)

    ancestry_records = chain(bundle_records, collection_records, nonaggregate_records)

    updates: Dict[str, Dict[str, Any]] = {}
    for record in ancestry_records:
        # Tee the stream of records into the accumulator, if one was provided (functional testing).
        if ancestry_records_accumulator is not None:
            ancestry_records_accumulator.append(record)

        if record.lidvid.is_collection() and len(record.parent_bundle_lidvids) == 0:
            log.warning(f"Collection {record.lidvid} is not referenced by any bundle.")

        doc_id = str(record.lidvid)
        update = {
            METADATA_PARENT_BUNDLE_KEY: [str(id) for id in record.parent_bundle_lidvids],
            METADATA_PARENT_COLLECTION_KEY: [str(id) for id in record.parent_collection_lidvids],
        }

        # Tee the stream of bulk update KVs into the accumulator, if one was provided (functional testing).
        if bulk_updates_sink is not None:
            bulk_updates_sink.append((doc_id, update))

        if doc_id in updates:
            existing_update = updates[doc_id]
            log.error(
                f"Multiple updates detected for doc_id {doc_id} - cannot create update! (got {update}, {existing_update} already exists)"
            )
            continue

        updates[doc_id] = update

    if updates and bulk_updates_sink is None:
        write_updated_docs(host, updates)

    # TODO: Search for and log any orphaned non-agg products - it's reasonably sufficient to just look for any product which lacks the relevant metadata keys.

    log.info("completed ancestry sweeper processing")


if __name__ == "__main__":
    cli_description = f"""
    Update registry records for non-latest LIDVIDs with up-to-date direct ancestry metadata ({METADATA_PARENT_BUNDLE_KEY} and {METADATA_PARENT_COLLECTION_KEY}).

    Retrieves existing published LIDVIDs from the registry, determines membership identities for each LID, and writes updated docs back to registry db
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
