from collections.abc import Mapping
from string import Template


class Replacer:

    def __init__(self, value_map: Mapping):
        self._value_map = value_map

    def get_filled_mapping(self, string, reader):
        stemplate = Template(string)
        try:
            filled_string = stemplate.substitute(**self._value_map)
        except KeyError:
            raise KeyError(f"Could not find value for token in settings mapping")
        return reader(filled_string)
