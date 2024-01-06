import os
from abc import ABC


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

    # Not yet implemented
    # db_write_timeout_seconds = int(os.environ.get('DB_WRITE_TIMEOUT_SECONDS'), 90)
