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