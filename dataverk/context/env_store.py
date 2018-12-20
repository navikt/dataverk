from pathlib import Path
from os import environ
from collections.abc import Mapping


class EnvStore(Mapping):

    def __init__(self, path: Path, env_setter=environ):
        tmp_dict = {}
        with path.open("r") as reader:
            envs = reader.readlines()

            for env in envs:
                env = env.strip()
                if env[0] == '#':
                    continue

                var = env.split('=')
                tmp_dict[str(var[0])] = str(var[1])

        self._env_store = {**env_setter, **tmp_dict}

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
        return self._env_store.items()

    def __contains__(self, o: object) -> bool:
        return super().__contains__(o)

    def __len__(self) -> int:
        return len(self._env_store)