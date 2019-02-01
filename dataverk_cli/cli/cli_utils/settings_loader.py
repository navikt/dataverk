import os
from urllib3.util.url import parse_url
from pathlib import Path
from distutils.dir_util import copy_tree
import requests
from git import Repo
import tempfile
import json
from dataverk.utils import file_functions


def load_settings_file_from_resource(url):
    """ Loads settings file from external resource """

    if _is_resource_git_repo(url=url):
        settings_dict = _get_settings_dict_from_git_repo(url)
    else:
        if _is_resource_web_hosted(url=url):
            settings_dict = _get_settings_dict_from_web_file(url)
        else:
            settings_dict = _get_settings_dict_from_local_file(url)

    return settings_dict


def load_template_files_from_resource(url):
    """ Loads template files from external resource """

    if _is_resource_git_repo(url=url):
        _get_templates_from_git_repo(url=url)


def _is_resource_git_repo(url: str):
    """
    Returns true if given url is a git repository

    """

    return _get_url_suffix(url) == "git"


def _get_url_suffix(url:str):
    return str(url).split(".")[-1]


def _is_resource_web_hosted(url: str):
    result = parse_url(url)
    if result.scheme == "http" or result.scheme == "https" or result.scheme == "ftp":
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


def _get_templates_from_git_repo(url):
    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            Repo.clone_from(url=url, to_path=tmpdir)
        except AttributeError:
            raise AttributeError(f"Could not clone git repository from url({url})")

        copy_tree(str(Path(tmpdir).joinpath('file_templates')), '.')


def _get_settings_dict_from_local_file(url):
    return file_functions.json_to_dict(url)


def _get_settings_dict_from_web_file(url):
        result = requests.get(url, allow_redirects=False)
        settings_dict = json.dumps(result.content)
        return settings_dict

