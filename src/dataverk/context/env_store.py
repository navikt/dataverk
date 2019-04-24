from pathlib import Path
from os import environ
from collections.abc import Mapping


class EnvStore(Mapping):
    """
    Mapping object for storing and easy accessing of Environment variables
    """

    def __init__(self, path: Path = None, env_setter=None):

        if env_setter is None:
            env_setter = environ

        tmp_dict = {}
        if path is not None:
            with path.open("r") as reader:
                envs = reader.readlines()

                for env in envs:
                    env = env.strip()
                    if env[0] == '#':
                        continue

                    var = env.split('=')
                    tmp_dict[str(var[0])] = str(var[1])

        self._env_store = {**env_setter, **tmp_dict}

    @classmethod
    def safe_create(cls, env_file):
        path = None
        if Path(env_file).exists():
            path = Path(env_file)
        return cls(path)

    def __getitem__(self, key: str):
        if not isinstance(key, str):
            raise ValueError("field should be a str")
        return self._env_store[key]

    def __iter__(self):
        return self._env_store.__iter__()

    def get(self, key):
        if not isinstance(key, str):
            raise ValueError("field should be a str")
        return self._env_store.get(key)

    def keys(self):
        return self._env_store.keys()

    def values(self):
        return self._env_store.values()

    def items(self):
        return self._env_store.items()

    def update(self, envs):
        self._env_store.update(envs)

    def __contains__(self, o: object) -> bool:
        return o in self._env_store

    def __len__(self) -> int:
        return len(self._env_store)
