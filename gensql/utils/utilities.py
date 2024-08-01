import random
from math import floor
from pathlib import PurePath
from typing import Dict, List, Union


def get_abs_path_to_so(file_path: str) -> str:
    _path = PurePath(file_path)
    return f"{_path.parent.parent}/bin/{_path.stem}.so"


def sample(iterable: list, num_rows: int, num_samples: int = 1) -> list[str] | str:
    sample_list = []
    for i in range(num_samples):
        idx = floor(random.random() * num_rows)
        if num_samples == 1:
            return iterable[idx]
        sample_list.append(iterable[idx])
    return sample_list


def lowercase_schema(
    schema: Union[Dict[str, str], str],
) -> Union[Dict[str, str], List[str], str]:
    """
    Allows input schemas to be correctly parsed if uppercase
    letters are used (e.g. NULL as a default) without doing repeated
    lower() calls during row creation.
    """
    if isinstance(schema, dict):
        return {k.lower(): lowercase_schema(v) for k, v in schema.items()}  # type: ignore
    elif isinstance(schema, list):
        return [lowercase_schema(v) for v in schema]
    elif isinstance(schema, str):
        return schema.lower()
    else:
        return schema


# Farewell, distutils
def strtobool(val) -> bool:
    """Convert a string representation of truth to true (1) or false (0).
    True values are "y", "yes", "t", "true", "on", and "1"; false values
    are "n", "no", "f", "false", "off", and "0".  Raises ValueError if
    "val" is anything else.
    """
    if val is None:
        return False
    try:
        val = val.lower()
    except AttributeError:
        val = str(val).lower()
    if val in ("y", "yes", "t", "true", "on", "1"):
        return True
    elif val in ("n", "no", "f", "false", "off", "0"):
        return False
    else:
        raise ValueError(f"invalid truth value {val}")
