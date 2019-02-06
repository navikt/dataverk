import os
import errno
from pathlib import Path
import json

def write_file(path, content):
    if not os.path.exists(os.path.dirname(path)):
        try:
            os.makedirs(os.path.dirname(path))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    with open(path, "w") as f:
        f.write(content)


def read_file(path):
    with open(path, mode="r", encoding="utf-8") as f:
        return f.read()


def json_to_dict(path: Path):
    file_string = read_file(path)
    return json.loads(file_string)


def _json_validate_params(file_path: Path):
    if not isinstance(file_path, Path):
        raise TypeError(f"settings_file_path: {file_path} should be a Path object")

    if not file_path.is_file():
        raise FileNotFoundError("The provided url does not resolve to a file")
    if _get_url_suffix(str(file_path)) != "json":
        raise FileNotFoundError("The provided url does not resolve to a json file")


def _get_url_suffix(url: str):
    return url.split(".")[-1]