import re
from dataverk.context.secset_replacer.finder import FileResourceFinder
from string import Template
import json


class Replacer:

    REGEX = r"\${([A-z0-9_/]*)}"

    def __init__(self, finder: FileResourceFinder):
        self._finder = finder

    def get_filled_dict(self, string):
        tokens = self._get_tokens(string)
        token_value_map = self._finder.create_filled_resource(tokens)
        stemplate = Template(string)
        filled_string = stemplate.safe_substitute(**token_value_map)
        return json.loads(filled_string)

    def _get_tokens(self, string):
        return re.findall(self.REGEX, string)
