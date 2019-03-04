from string import Template
import json

from dataverk.context.secset_replacer.secrets_importer import SecretsImporter


class Replacer:

    def __init__(self, importer: SecretsImporter):
        self._finder = importer

    def get_filled_mapping(self, string):
        secret_name_value_map = self._finder.import_secrets()
        stemplate = Template(string)
        try:
            filled_string = stemplate.substitute(**secret_name_value_map)
        except KeyError:
            raise KeyError(f"Could not find secret for token in settings mapping")
        return json.loads(filled_string)
