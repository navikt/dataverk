class EnvironmentVariableNotSet(Exception):

    def __init__(self, env: str):
        self._env = env

    def __str__(self):
        return f"""
                Required environment variable {self._env} is not set
                """


class IncompleteSettingsObject(Exception):

    def __init__(self, message: str):
        self._message = message

    def __str__(self):
        return self._message


class StorageBucketDoesNotExist(Exception):

    def __init__(self, bucket_name):
        self._bucket_name = bucket_name

    def __str__(self):
        return f"""{self._bucket_name} does not exist"""