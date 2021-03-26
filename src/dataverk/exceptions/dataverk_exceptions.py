import json
from dataverk.utils import dataverk_doc_address


class EnvironmentVariableNotSet(Exception):

    def __init__(self, env: str):
        self._env = env

    def __str__(self):
        return f"""
                Required environment variable {self._env} is not set
                """


class IncompleteSettingsObject(Exception):

    def __init__(self, missing: str):
        self._missing = missing

    def __str__(self):
        return f"""{self._missing} is not provided in settings object. 
        See {dataverk_doc_address} for guidelines on how to setup settings file."""


class StorageBucketDoesNotExist(Exception):

    def __init__(self, bucket_name):
        self._bucket_name = bucket_name

    def __str__(self):
        return f"""{self._bucket_name} does not exist"""


class ElasticSearchApiError(Exception):

    def __init__(self, error_message: str, details):
        self._error_message = error_message
        self._details = []

        if isinstance(json.loads(details)["detail"], list):
            for param in json.loads(details)["detail"]:
                self._details.append(self._format_error(param))
        else:
            self._details.append(details)

    @staticmethod
    def _format_error(param: dict):
        try:
            return f"{param['loc'][1]}: {param['msg']}"
        except (KeyError, IndexError):
            return json.dumps(param)

    def __str__(self):
        error_str = self._error_message + "\n\n"

        error_str += "Affected metadata fields:"
        for detail in self._details:
            error_str += "\n" + detail

        return error_str
