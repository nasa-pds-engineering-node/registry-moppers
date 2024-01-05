import logging
from enum import auto
from enum import Enum
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import Optional

from opensearchpy import OpenSearch
from pds.registrysweepers.utils.db import query_registry_db_or_mock

log = logging.getLogger(__name__)

DbMockTypeDef = Optional[Callable[[str], Iterable[Dict]]]


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

    return {"query": queries[cls]}


def get_bundle_ancestry_records_query(client: OpenSearch, db_mock: DbMockTypeDef = None) -> Iterable[Dict]:
    query = product_class_query_factory(ProductClass.BUNDLE)
    _source = {"includes": ["lidvid"]}
    query_f = query_registry_db_or_mock(db_mock, "get_bundle_ancestry_records", use_search_after=True)
    docs = query_f(client, query, _source)

    return docs


def get_collection_ancestry_records_bundles_query(client: OpenSearch, db_mock: DbMockTypeDef = None) -> Iterable[Dict]:
    query = product_class_query_factory(ProductClass.BUNDLE)
    _source = {"includes": ["lidvid", "ref_lid_collection"]}
    query_f = query_registry_db_or_mock(db_mock, "get_collection_ancestry_records_bundles", use_search_after=True)
    docs = query_f(client, query, _source)

    return docs


def get_collection_ancestry_records_collections_query(
    client: OpenSearch, db_mock: DbMockTypeDef = None
) -> Iterable[Dict]:
    # Query the registry for all collection identifiers
    query = product_class_query_factory(ProductClass.COLLECTION)
    _source = {"includes": ["lidvid", "alternate_ids"]}
    query_f = query_registry_db_or_mock(db_mock, "get_collection_ancestry_records_collections", use_search_after=True)
    docs = query_f(client, query, _source)

    return docs


def get_nonaggregate_ancestry_records_query(client: OpenSearch, registry_db_mock: DbMockTypeDef) -> Iterable[Dict]:
    # Query the registry-refs index for the contents of all collections
    query: Dict = {"query": {"match_all": {}}}
    _source = {"includes": ["collection_lidvid", "batch_id", "product_lidvid"]}
    query_f = query_registry_db_or_mock(registry_db_mock, "get_nonaggregate_ancestry_records", use_search_after=True)

    # each document will have many product lidvids, so a smaller page size is warranted here
    docs = query_f(
        client,
        query,
        _source,
        index_name="registry-refs",
        page_size=2000,
        request_timeout_seconds=30,
        sort_fields=["collection_lidvid", "batch_id"],
    )

    return docs
