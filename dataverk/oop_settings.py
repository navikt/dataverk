from pathlib import Path
import json


class Settings:
    """ Klassen har ansvar for å gjøre eksterne ressurser tilgjengelige.

        Den leser av miljø variabler og settings.json filen for å generere propterties som kan brukes til å nå eksterne
        ressurser.

    """

    def __init__(self, settings_json_url: Path):
        self._validate_settings_file(settings_json_url)
        self._settings_file_path = settings_json_url
        self._set_data_field()

    def _validate_settings_file(self, url: Path):
        if not url.is_file():
            raise FileNotFoundError("The provided url does not resolve to a file")
        if self._get_url_suffix(str(url)) != "json":
            raise FileNotFoundError("The provided url does not resolve to a json file")

    def _get_url_suffix(self, url:str):
        return url.split(".")[-1]

    def _set_data_field(self):
        self._settings_dict = self._json_to_dict()

    def _json_to_dict(self):
        return json.loads(self._read_file())

    def _read_file(self):
        with self._settings_file_path.open("r") as reader:
            return reader.read()

    def get_field(self, field: str):
        if not isinstance(field, str):
            raise ValueError("field should be a str")
        return self._settings_dict[field]


