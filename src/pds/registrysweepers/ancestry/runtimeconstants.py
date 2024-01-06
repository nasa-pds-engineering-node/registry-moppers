import os
from abc import ABC

from pds.registrysweepers.utils.misc import parse_boolean_env_var


class AncestryRuntimeConstants(ABC):
    # how many registry-refs documents (each collection has multiple docs for batches of member non-aggregates)
    # Decrease to reduce peak memory demand - increases runtime
    nonaggregate_ancestry_records_query_page_size: int = int(
        os.environ.get("ANCESTRY_NONAGGREGATE_QUERY_PAGE_SIZE", 2000)
    )

    # how many nonaggregate history records should be processed before dumping to disk?
    # Decrease to reduce peak memory demand - increases runtime
    nonaggregate_records_disk_dump_threshold: int = int(
        os.environ.get("ANCESTRY_NONAGGREGATE_DISK_DUMP_THRESHOLD", 500000)
    )

    # Expects a value like "true" or "1"
    disable_chunking: bool = parse_boolean_env_var("ANCESTRY_DISABLE_CHUNKING")

    # Not yet implemented
    # db_write_timeout_seconds = int(os.environ.get('DB_WRITE_TIMEOUT_SECONDS'), 90)
