import random
from datetime import datetime
from typing import Any
from typing import Callable
from typing import List


def coerce_list_type(db_value: Any) -> List[Any]:
    """
    Coerce a non-array-typed legacy db record into a list containing itself as the only element, or return the
    original argument if it is already an array (list).  This is sometimes necessary to support legacy db records which
    did not wrap singleton properties in an enclosing array.
    """

    return (
        db_value
        if type(db_value) is list
        else [
            db_value,
        ]
    )


def get_human_readable_elapsed_since(begin: datetime) -> str:
    elapsed_seconds = (datetime.now() - begin).total_seconds()
    h = int(elapsed_seconds / 3600)
    m = int(elapsed_seconds % 3600 / 60)
    s = int(elapsed_seconds % 60)
    return (f"{h}h" if h else "") + (f"{m}m" if m else "") + f"{s}s"


def get_random_hex_id(id_len: int = 6) -> str:
    val = random.randint(0, 16**id_len)
    return hex(val)[2:]


def auto_raise_for_status(f: Callable) -> Callable:
    """
    Given a function requests.{verb}, return a version of it which will automatically raise_for_status().  This is
    used with retry.retry_call() in cirumstances where it is not desirable to extract the retryable block to its own
    function, for example in a loop which shares a lot of state with its enclosing scope.
    """

    def wrapped_f(*args, **kwargs):
        resp = f(*args, **kwargs)
        resp.raise_for_status()
        return resp

    return wrapped_f


def get_sweeper_version_metadata_key(sweeper_name: str) -> str:
    return f"ops:Provenance/ops:registry_sweepers_{sweeper_name}_version"
