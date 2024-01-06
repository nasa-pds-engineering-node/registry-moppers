import json
import logging
import os
from datetime import datetime
from typing import Dict
from typing import Iterable
from typing import List
from typing import Union

from pds.registrysweepers.ancestry import AncestryRecord
from pds.registrysweepers.ancestry.typedefs import SerializableAncestryRecordTypeDef
from pds.registrysweepers.utils.productidentifiers.pdslidvid import PdsLidVid

log = logging.getLogger(__name__)


def make_history_serializable(history: Dict[PdsLidVid, AncestryRecord]) -> Dict[str, SerializableAncestryRecordTypeDef]:
    """Destructively convert history into something able to be dumped to JSON"""
    log.debug(f"Converting history into serializable types...")
    serializable_history: Dict[str, SerializableAncestryRecordTypeDef] = {}
    for old_k in list(history.keys()):
        old_v = history.pop(old_k)
        new_k = str(old_k)
        new_v = old_v.to_dict()
        serializable_history[new_k] = new_v
    log.debug("    complete!")
    return serializable_history


def dump_history_to_disk(parent_dir: str, history: Dict[str, SerializableAncestryRecordTypeDef]):
    temp_fp = os.path.join(parent_dir, datetime.now().isoformat().replace(":", "-"))
    log.info(f"Dumping history to {temp_fp} for later merging...")
    with open(temp_fp, "w+") as outfile:
        json.dump(history, outfile)
    log.debug("    complete!")


def merge_matching_history_chunks(dest_fp: str, src_fps: Iterable[str]):
    log.info(f"Performing merges into {dest_fp}")
    with open(dest_fp, "r") as dest_infile:
        dest_file_content: Dict[str, SerializableAncestryRecordTypeDef] = json.load(dest_infile)

    for src_fn in src_fps:
        log.debug(f"merging from {src_fn}...")
        with open(src_fn, "r") as src_infile:
            src_file_content: Dict[str, SerializableAncestryRecordTypeDef] = json.load(src_infile)

        # For every lidvid with history in the "active" file, absorb all relevant history from this inactive file
        for lidvid_str, dest_history_entry in dest_file_content.items():
            try:
                src_history_to_merge = src_file_content[lidvid_str]
                src_file_content.pop(lidvid_str)

                dest_history_entry = dest_file_content[lidvid_str]
                for k in ["parent_bundle_lidvids", "parent_collection_lidvids"]:
                    dest_history_entry[k].extend(src_history_to_merge[k])  # type: ignore

            except KeyError:
                # If the src history doesn't contain history for this lidvid, there's nothing to do
                pass

        # Overwrite the content of the source file with any remaining history not absorbed
        with open(src_fn, "w+") as src_outfile:
            json.dump(src_file_content, src_outfile)

    # Overwrite the content of the destination file with any remaining history not absorbed
    with open(dest_fp, "w+") as src_outfile:
        json.dump(dest_file_content, src_outfile)

    log.debug("    complete!")


def load_partial_history_to_records(fn: str) -> Iterable[AncestryRecord]:
    with open(fn, "r") as infile:
        content: Dict[str, SerializableAncestryRecordTypeDef] = json.load(infile)

    for history_dict in content.values():
        yield AncestryRecord.from_dict(history_dict)
