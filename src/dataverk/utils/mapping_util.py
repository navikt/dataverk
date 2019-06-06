from collections import Mapping


def safe_get_nested(mapping: Mapping, keys: tuple, default):
    for key in keys:
        if key not in mapping:
            return default
        else:
            mapping = mapping[key]
    return mapping