import gzip
import io
import os
import json
import pathlib
import shutil
import stat
import requests
import re

from pathlib import Path
from urllib3.exceptions import LocationParseError
from urllib3.util import url

from dataverk.connectors.storage.google_storage import GoogleStorageConnector


def get_package_resource(resource_name: str, base_path: str, http_headers: dict):
    if os.environ.get("DATAVERK_SETTINGS_PATH", None) is not None:
        return read_file(Path(os.environ.get("DATAVERK_SETTINGS_PATH")).joinpath(resource_name))
    else:
        try:
            parsed_url = url.parse_url(base_path)
        except LocationParseError:
            # parse_url throws exception with local windows paths
            return read_file(Path(base_path).joinpath(resource_name))
        else:
            if parsed_url.scheme == "http" or parsed_url.scheme == "https":
                return requests.get(f"{base_path}/{resource_name}",
                                    headers=http_headers).text
            elif parsed_url.scheme == "gs":
                bucket_conn = GoogleStorageConnector(bucket_name=parsed_url.host)
                return bucket_conn.read(f"{parsed_url.path[1:]}/{resource_name}")
            else:
                return read_file(Path(base_path).joinpath(resource_name))


def write_file(path, content, compressed: bool=False):
    pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)
    with Path(path).open("w" if not compressed else "wb") as file:
        file.write(content)


def read_file(path, encoding="utf-8"):
    with Path(path).open("r", encoding=encoding) as file:
        return file.read()


def json_to_dict(path: Path):
    file_string = read_file(path)
    return json.loads(file_string)


def remove_special_characters(filename):
    pattern = "[^a-zA-Z0-9_-]"
    filename_wo_spaces = filename.replace(' ', '_')
    return re.sub(pattern, "", filename_wo_spaces)


def _json_validate_params(file_path: Path):
    if not isinstance(file_path, Path):
        raise TypeError(f"settings_file_path: {file_path} should be a Path object")

    if not file_path.is_file():
        raise FileNotFoundError("The provided url does not resolve to a file")
    if _get_url_suffix(str(file_path)) != "json":
        raise FileNotFoundError("The provided url does not resolve to a json file")


def _get_url_suffix(url: str):
    return url.split(".")[-1]


def win_readonly_directories_force_remove(directory):
    """ Removes directories with read-only files"""
    shutil.rmtree(directory, onerror=_remove_readonly)


def _remove_readonly(func, path, _):
    """Clear the readonly bit and reattempt the removal"""
    os.chmod(path, stat.S_IWRITE)
    func(path)


def compress_content(data_buff):
    gz_buff = io.BytesIO()
    with gzip.GzipFile(fileobj=gz_buff, mode='w') as zipped_f:
        zipped_f.write(bytes(data_buff.getvalue(), encoding="utf-8-sig"))
    return gz_buff.getvalue()
