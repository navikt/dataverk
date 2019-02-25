from pathlib import Path
from collections.abc import Mapping


class FileResourceFinder(Mapping):

    def __init__(self, resource):
        self._resource = Path(resource)

    def create_filled_resource(self, token_file_map):
        return self._fetch_resource_values(token_file_map)

    def _fetch_resource_values(self, tokens):

        token_value_map = {}
        for token in tokens:
            f = self._resource.joinpath(token).open("r")
            token_value_map[token] = f.read()
            f.close()
        return token_value_map


