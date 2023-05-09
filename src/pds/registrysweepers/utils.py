import argparse
import collections
import json
import logging
import urllib.parse
from argparse import Namespace
from typing import Dict
from typing import Iterable
from typing import List
from typing import Mapping
from typing import Union

import requests

HOST = collections.namedtuple("HOST", ["cross_cluster_remotes", "password", "url", "username", "verify"])

log = logging.getLogger("registrysweepers.utils")


def parse_args(description: str = "", epilog: str = "") -> Namespace:
    """
    Provides a consistent CLI for sweepers.  May need to be re-thought in future but a standardized interface makes
    sense for the time being.
    """
    ap = argparse.ArgumentParser(
        description=description,
        epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("-b", "--base-URL", required=True, type=str)
    ap.add_argument(
        "-c",
        "--ccs-remotes",
        default=[],
        nargs="*",
        help="names of additional opensearch cross-cluster remotes, space-separated",
    )
    ap.add_argument("-l", "--log-file", default=None, required=False, help="file to write the log messages")
    ap.add_argument(
        "-L",
        "--log-level",
        default="ERROR",
        required=False,
        type=parse_log_level,
        help="Python logging level as an int or string like INFO for logging.INFO [%(default)s]",
    )
    ap.add_argument(
        "-p",
        "--password",
        default=None,
        required=False,
        help="password to login to opensearch leaving it blank if opensearch does not require login",
    )
    ap.add_argument(
        "-u",
        "--username",
        default=None,
        required=False,
        help="username to login to opensearch leaving it blank if opensearch does not require login",
    )
    ap.add_argument("-v", "--verify", action="store_true", default=False, help="verify the host certificates")

    args = ap.parse_args()
    return args


def parse_log_level(input: str) -> int:
    """Given a numeric or uppercase descriptive log level, return the associated int"""
    try:
        result = int(input)
    except ValueError:
        result = getattr(logging, input)
    return result


def _vid_as_tuple_of_int(lidvid: str):
    major_version, minor_version = lidvid.split("::")[1].split(".")
    return (int(major_version), int(minor_version))


def configure_logging(filepath: Union[str, None], log_level: int):
    logging.root.handlers = []
    handlers: List[logging.StreamHandler] = [logging.StreamHandler()]

    if filepath:
        handlers.append(logging.FileHandler(filepath))

    logging.basicConfig(level=log_level, format="%(asctime)s::%(levelname)s::%(message)s", handlers=handlers)


def get_extant_lidvids(host: HOST) -> Iterable[str]:
    """
    Given an OpenSearch host, return all extant LIDVIDs
    """

    log.info("Retrieving extant LIDVIDs")

    cross_cluster_indexes = [node + ":registry" for node in host.cross_cluster_remotes]
    path = ",".join(["registry"] + cross_cluster_indexes) + "/_search?scroll=10m"
    extant_lidvids = []
    query = {
        "query": {"bool": {"must": [{"terms": {"ops:Tracking_Meta/ops:archive_status": ["archived", "certified"]}}]}},
        "_source": {"includes": ["lidvid"]},
        "size": 10000,
    }

    more_data_exists = True
    while more_data_exists:
        resp = requests.get(
            urllib.parse.urljoin(host.url, path), auth=(host.username, host.password), verify=host.verify, json=query
        )
        resp.raise_for_status()

        data = resp.json()
        path = "_search/scroll"
        query = {"scroll": "10m", "scroll_id": data["_scroll_id"]}
        extant_lidvids.extend([hit["_source"]["lidvid"] for hit in data["hits"]["hits"]])
        more_data_exists = len(extant_lidvids) < data["hits"]["total"]["value"]

        hits = data["hits"]["total"]["value"]
        percent_hit = int(round(len(extant_lidvids) / hits * 100))
        log.info(f"   ...{len(extant_lidvids)} of {hits} retrieved ({percent_hit}%)...")

    if "scroll_id" in query:
        path = f'_search/scroll/{query["scroll_id"]}'
        requests.delete(urllib.parse.urljoin(host.url, path), auth=(host.username, host.password), verify=host.verify)

    log.info("Finished retrieving LIDVIDs with current direct successors!")

    return extant_lidvids


def write_updated_docs(host: HOST, ids_and_updates: Mapping[str, Dict]):
    """
    Given an OpenSearch host and a mapping of doc ids onto updates to those docs, write bulk updates to documents in db.
    """
    log.info("Bulk update %d documents", len(ids_and_updates))

    bulk_updates = []
    ccs_indexes = [node + ":registry" for node in host.cross_cluster_remotes]
    headers = {"Content-Type": "application/x-ndjson"}
    path = ",".join(["registry"] + ccs_indexes) + "/_bulk"

    for lidvid, update_content in ids_and_updates.items():
        bulk_updates.append(json.dumps({"update": {"_id": lidvid}}))
        bulk_updates.append(json.dumps({"doc": update_content}))

    bulk_data = "\n".join(bulk_updates) + "\n"

    log.info(f"writing bulk update for {len(bulk_updates)} products...")
    response = requests.put(
        urllib.parse.urljoin(host.url, path),
        auth=(host.username, host.password),
        data=bulk_data,
        headers=headers,
        verify=host.verify,
    )
    response.raise_for_status()

    response_content = response.json()
    if response_content.get("errors"):
        for item in response_content["items"]:
            if "error" in item:
                log.error("update error (%d): %s", item["status"], str(item["error"]))
    else:
        log.info("bulk updates were successful")
