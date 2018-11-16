from pathlib import Path


class EnvStore:

    def __init__(self, path: Path):
        tmp_dict = {}
        with path.open("r") as reader:
            envs = reader.readlines()

            for env in envs:
                env = env.strip()
                if env[0] == '#':
                    continue

                var = env.split('=')
                tmp_dict[str(var[0])] = str(var[1])
        self._env_store = tmp_dict

    def __getitem__(self, field: str):
        if not isinstance(field, str):
            raise ValueError("field should be a str")
        return self._env_store[field]
