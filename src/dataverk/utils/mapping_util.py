from typing import Sequence, Mapping


def safe_get_nested(mapping: Mapping, keys: Sequence, default):
    for key in keys:
        if key not in mapping:
            return default
        else:
            mapping = mapping[key]
    return mapping