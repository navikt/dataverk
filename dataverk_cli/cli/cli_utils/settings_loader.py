import os
from urllib3.util.url import parse_url
from pathlib import Path
from shutil import copyfile
import requests
from git import Repo
import tempfile
import json
from dataverk.utils import file_functions


def load_settings_file_from_resource(url):
    """ Loads settings file from external resource """

    if _is_location_git_repo(url=url):
        settings_dict = _get_settings_dict_from_git_repo(url)
    else:
        if _is_online_resource(url=url):
            settings_dict = _get_settings_dict_from_web_file(url)
        else:
            settings_dict = _get_settings_dict_from_local_file(url)

    return settings_dict


def _is_location_git_repo(url: str):
    """
    Returns true if given url is a git repository

    """

    return _get_url_suffix(url) == "git"


def _get_url_suffix(url:str):
    return url.split(".")[-1]


def _is_online_resource(url: str):
    result = parse_url(url)
    if result.scheme == "http" or result.scheme == "https":
        return True
    else:
        return False


def _get_settings_dict_from_git_repo(url):
    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            Repo.clone_from(url=url, to_path=tmpdir)
        except AttributeError:
            raise AttributeError(f"Could not clone git repository from url({url})")

        settings_file = Path(tmpdir).joinpath("settings.json")
        json_str = file_functions.read_file(settings_file)
        settings_dict = json.loads(json_str)
        return settings_dict


def _get_settings_dict_from_local_file(url):
    return file_functions.json_to_dict(url)


def _get_settings_dict_from_web_file(url):
        result = requests.get(url, allow_redirects=False)
        settings_dict = json.dumps(result.content)
        return settings_dict

