import logging
from typing import Union

import elasticsearch.helpers  # type: ignore
from elasticsearch import Elasticsearch
from pds.registrysweepers.legacy_registry_sync.opensearch_loaded_product import get_already_loaded_lidvids
from pds.registrysweepers.legacy_registry_sync.solr_doc_export_to_opensearch import SolrOsWrapperIter
from pds.registrysweepers.utils import configure_logging
from solr_to_es.solrSource import SlowSolrDocs  # type: ignore

log = logging.getLogger(__name__)

SOLR_URL = "https://pds.nasa.gov/services/search/search"
OS_INDEX = "legacy_registry"


def create_legacy_registry_index(es_conn=None):
    """
    Creates if not already created the legacy_registry index.

    @param es_conn: elasticsearch.ElasticSearch instance for the ElasticSearch or OpenSearch connection
    @return:
    """
    if not es_conn.indices.exists(OS_INDEX):
        log.info("create index %s", OS_INDEX)
        es_conn.indices.create(index=OS_INDEX, body={})
    log.info("index created %s", OS_INDEX)


def run(
    base_url: str,
    username: str,
    password: str,
    verify_host_certs: bool = True,
    log_filepath: Union[str, None] = None,
    log_level: int = logging.INFO,
):
    """
    Runs the Solr Legacy Registry synchronization with OpenSearch.

    @param base_url: of the OpenSearch service
    @param username:
    @param password:
    @param verify_host_certs:
    @param log_filepath:
    @param log_level:
    @return:
    """

    configure_logging(filepath=log_filepath, log_level=log_level)

    es_conn = Elasticsearch(hosts=base_url, verify_certs=verify_host_certs, http_auth=(username, password))

    solr_itr = SlowSolrDocs(SOLR_URL, "*", rows=100)

    create_legacy_registry_index(es_conn=es_conn)

    prod_ids = get_already_loaded_lidvids(
        product_classes=["Product_Context", "Product_Collection", "Product_Bundle"], es_conn=es_conn
    )

    es_actions = SolrOsWrapperIter(solr_itr, OS_INDEX, found_ids=prod_ids)
    for ok, item in elasticsearch.helpers.streaming_bulk(
        es_conn, es_actions, chunk_size=50, max_chunk_bytes=50000000, max_retries=5, initial_backoff=10
    ):
        if not ok:
            log.error(item)