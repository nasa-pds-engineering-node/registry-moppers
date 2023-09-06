import json
import logging
import sys
import urllib.parse
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import List
from typing import Mapping
from typing import Optional

import requests
from pds.registrysweepers.utils.db.host import Host
from pds.registrysweepers.utils.db.update import Update
from pds.registrysweepers.utils.misc import auto_raise_for_status
from pds.registrysweepers.utils.misc import get_random_hex_id
from requests import HTTPError
from retry import retry
from retry.api import retry_call

log = logging.getLogger(__name__)


def query_registry_db(
    host: Host,
    query: Dict,
    _source: Dict,
    index_name: str = "registry",
    page_size: int = 10000,
    scroll_keepalive_minutes: int = 10,
) -> Iterable[Dict]:
    """
    Given an OpenSearch host and query/_source, return an iterable collection of hits

    Example query: {"bool": {"must": [{"terms": {"ops:Tracking_Meta/ops:archive_status": ["archived", "certified"]}}]}}
    Example _source: {"includes": ["lidvid"]}
    """

    req_content = {
        "query": query,
        "_source": _source,
        "size": page_size,
    }

    query_id = get_random_hex_id()  # This is just used to differentiate queries during logging
    log.info(f"Initiating query with id {query_id}: {req_content}")

    path = f"{index_name}/_search?scroll={scroll_keepalive_minutes}m"

    served_hits = 0

    last_info_log_at_percentage = 0
    log.info(f"Query {query_id} progress: 0%")

    more_data_exists = True
    while more_data_exists:
        resp = retry_call(
            auto_raise_for_status(requests.get),
            fargs=[urllib.parse.urljoin(host.url, path)],
            fkwargs={"auth": (host.username, host.password), "verify": host.verify, "json": req_content},
            exceptions=(HTTPError, RuntimeError),
            tries=6,
            delay=2,
            backoff=2,
            logger=log,
        )

        data = resp.json()
        path = "_search/scroll"
        req_content = {"scroll": f"{scroll_keepalive_minutes}m", "scroll_id": data["_scroll_id"]}

        total_hits = data["hits"]["total"]["value"]
        log.debug(
            f"   paging query {query_id} ({served_hits} to {min(served_hits + page_size, total_hits)} of {total_hits})"
        )

        response_hits = data["hits"]["hits"]
        for hit in response_hits:
            served_hits += 1

            percentage_of_hits_served = int(served_hits / total_hits * 100)
            if last_info_log_at_percentage is None or percentage_of_hits_served >= (last_info_log_at_percentage + 5):
                last_info_log_at_percentage = percentage_of_hits_served
                log.info(f"Query {query_id} progress: {percentage_of_hits_served}%")

            yield hit

        # This is a temporary, ad-hoc guard against empty/erroneous responses which do not return non-200 status codes.
        # Previously, this has cause infinite loops in production due to served_hits sticking and never reaching the
        # expected total hits value.
        # TODO: Remove this upon implementation of https://github.com/NASA-PDS/registry-sweepers/issues/42
        hits_data_present_in_response = len(response_hits) > 0
        if not hits_data_present_in_response:
            log.error(
                f"Response for query {query_id} contained no hits when hits were expected.  Returned data is incomplete (got {served_hits} of {total_hits} total hits).  Response was: {data}"
            )
            break

        more_data_exists = served_hits < data["hits"]["total"]["value"]

    # TODO: Determine if the following block is actually necessary
    if "scroll_id" in req_content:
        path = f'_search/scroll/{req_content["scroll_id"]}'
        retry_call(
            auto_raise_for_status(requests.delete),
            fargs=[urllib.parse.urljoin(host.url, path)],
            fkwargs={"auth": (host.username, host.password), "verify": host.verify},
            tries=6,
            delay=2,
            backoff=2,
            logger=log,
        )

    log.info(f"Query {query_id} complete!")


def query_registry_db_or_mock(mock_f: Optional[Callable[[str], Iterable[Dict]]], mock_query_id: str):
    if mock_f is not None:

        def mock_wrapper(
            host: Host,
            query: Dict,
            _source: Dict,
            index_name: str = "registry",
            page_size: int = 10000,
            scroll_validity_duration_minutes: int = 10,
        ) -> Iterable[Dict]:
            return mock_f(mock_query_id)  # type: ignore  # see None-check above

        return mock_wrapper
    else:
        return query_registry_db


def write_updated_docs(host: Host, updates: Iterable[Update], index_name: str = "registry"):
    log.info("Updating a lazily-generated collection of product documents...")
    updated_doc_count = 0

    bulk_buffer_max_size_mb = 30.0
    bulk_buffer_size_mb = 0.0
    bulk_updates_buffer: List[str] = []
    for update in updates:
        if bulk_buffer_size_mb > bulk_buffer_max_size_mb:
            pending_product_count = int(len(bulk_updates_buffer) / 2)
            log.info(
                f"Bulk update buffer has reached {bulk_buffer_max_size_mb}MB threshold - writing {pending_product_count} document updates to db..."
            )
            _write_bulk_updates_chunk(host, index_name, bulk_updates_buffer)
            bulk_updates_buffer = []
            bulk_buffer_size_mb = 0.0

        update_statement_strs = update_as_statements(update)

        for s in update_statement_strs:
            bulk_buffer_size_mb += sys.getsizeof(s) / 1024**2

        bulk_updates_buffer.extend(update_statement_strs)
        updated_doc_count += 1

    remaining_products_to_write_count = int(len(bulk_updates_buffer) / 2)
    updated_doc_count += remaining_products_to_write_count

    if len(bulk_updates_buffer) > 0:
        log.info(f"Writing documents updates for {remaining_products_to_write_count} remaining products to db...")
        _write_bulk_updates_chunk(host, index_name, bulk_updates_buffer)

    log.info(f"Updated documents for {updated_doc_count} total products!")


def update_as_statements(update: Update) -> Iterable[str]:
    """Given an Update, convert it to an ElasticSearch-style set of request body content strings"""
    update_objs = [{"update": {"_id": update.id}}, {"doc": update.content}]
    updates_strs = [json.dumps(obj) for obj in update_objs]
    return updates_strs


@retry(exceptions=(HTTPError, RuntimeError), tries=6, delay=2, backoff=2, logger=log)
def _write_bulk_updates_chunk(host: Host, index_name: str, bulk_updates: Iterable[str]):
    headers = {"Content-Type": "application/x-ndjson"}
    path = f"{index_name}/_bulk"

    bulk_data = "\n".join(bulk_updates) + "\n"

    response = requests.put(
        urllib.parse.urljoin(host.url, path),
        auth=(host.username, host.password),
        data=bulk_data,
        headers=headers,
        verify=host.verify,
    )

    # N.B. HTTP status 200 is insufficient as a success check for _bulk API.
    # See: https://github.com/elastic/elasticsearch/issues/41434
    response.raise_for_status()
    response_content = response.json()
    if response_content.get("errors"):
        warn_types = {"document_missing_exception"}  # these types represent bad data, not bad sweepers behaviour
        items_with_problems = [item for item in response_content["items"] if "error" in item["update"]]

        if log.isEnabledFor(logging.WARNING):
            items_with_warnings = [
                item for item in items_with_problems if item["update"]["error"]["type"] in warn_types
            ]
            warning_aggregates = aggregate_update_error_types(items_with_warnings)
            for error_type, reason_aggregate in warning_aggregates.items():
                for error_reason, ids in reason_aggregate.items():
                    log.warning(
                        f"Attempt to update the following documents failed due to {error_type} ({error_reason}): {ids}"
                    )

        if log.isEnabledFor(logging.ERROR):
            items_with_errors = [
                item for item in items_with_problems if item["update"]["error"]["type"] not in warn_types
            ]
            error_aggregates = aggregate_update_error_types(items_with_errors)
            for error_type, reason_aggregate in error_aggregates.items():
                for error_reason, ids in reason_aggregate.items():
                    log.error(
                        f"Attempt to update the following documents failed unexpectedly due to {error_type} ({error_reason}): {ids}"
                    )


def aggregate_update_error_types(items: Iterable[Dict]) -> Mapping[str, Dict[str, List[str]]]:
    """Return a nested aggregation of ids, aggregated first by error type, then by reason"""
    agg: Dict[str, Dict[str, List[str]]] = {}
    for item in items:
        id = item["update"]["_id"]
        error = item["update"]["error"]
        error_type = error["type"]
        error_reason = error["reason"]
        if error_type not in agg:
            agg[error_type] = {}

        if error_reason not in agg[error_type]:
            agg[error_type][error_reason] = []

        agg[error_type][error_reason].append(id)

    return agg


def get_extant_lidvids(host: Host) -> Iterable[str]:
    """
    Given an OpenSearch host, return all extant LIDVIDs
    """

    log.info("Retrieving extant LIDVIDs")

    query = {"bool": {"must": [{"terms": {"ops:Tracking_Meta/ops:archive_status": ["archived", "certified"]}}]}}
    _source = {"includes": ["lidvid"]}

    results = query_registry_db(host, query, _source, scroll_keepalive_minutes=1)

    return map(lambda doc: doc["_source"]["lidvid"], results)
