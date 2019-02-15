import os
from urllib3.util.url import parse_url, LocationParseError
from pathlib import Path
from distutils.dir_util import copy_tree
import requests
from git import Repo
from git.exc import GitCommandError, GitError
import tempfile
import json
from dataverk.utils import file_functions


def load_settings_file_from_resource(resource):
    """ Loads settings file from external resource """

    if _is_resource_git_repo(url=resource):
        settings_dict = _get_settings_dict_from_git_repo(resource)
    elif _is_resource_web_hosted(url=resource):
        settings_dict = _get_settings_dict_from_web_file(resource)
    else:
        settings_dict = _get_settings_dict_from_local_file(resource)

    return settings_dict


def load_template_files_from_resource(url):
    """ Loads template files from external resource """

    if _is_resource_git_repo(url=url):
        _get_templates_from_git_repo(url=url)
    else:
        raise NotImplementedError(f"url={url} is an unsupported resource type")


def _is_resource_git_repo(url: str):
    """
    Returns true if given url is a git repository

    """

    return _get_url_suffix(url) == "git"


def _get_url_suffix(url: str):
    return str(url).split(".")[-1]


def _is_resource_web_hosted(url: str):
    try:
        result = parse_url(str(url))
    except LocationParseError:
        return False
    else:
        if result.scheme == "http" or result.scheme == "https" or result.scheme == "ftp":
            return True
        else:
            return False


def _get_settings_dict_from_git_repo(url):

    tmpdir = tempfile.TemporaryDirectory()
    try:
        Repo.clone_from(url=str(url), to_path=tmpdir.name)
        settings_file = Path(tmpdir.name).joinpath("settings.json")
        json_str = file_functions.read_file(settings_file)
        settings_dict = json.loads(json_str)
        return settings_dict
    except (AttributeError, GitError):
        raise AttributeError(f"Could not clone git repository from url({url})")
    finally:
        try:
            _windows_specific_cleanup(tmpdir=tmpdir)
            tmpdir.cleanup()
        except FileNotFoundError:
            pass


def _get_templates_from_git_repo(url):
    tmpdir = tempfile.TemporaryDirectory()
    try:
        Repo.clone_from(url=url, to_path=tmpdir.name)
        copy_tree(str(Path(tmpdir.name).joinpath('file_templates')), '.')
    except AttributeError:
        raise AttributeError(f"Could not clone git repository from url({url})")
    finally:
        try:
            _windows_specific_cleanup(tmpdir=tmpdir)
            tmpdir.cleanup()
        except FileNotFoundError:
            pass


def _get_settings_dict_from_local_file(url):
    return file_functions.json_to_dict(url)


def _get_settings_dict_from_web_file(url):
    result = requests.get(url, allow_redirects=False)
    settings_dict = json.dumps(result.content)
    return settings_dict


def _windows_specific_cleanup(tmpdir):
    """ Windows can stop the tempdir removal process
        This function handles that case
    """
    tmp_path = Path(tmpdir.name)
    for dir in tmp_path.iterdir():
        try:
            file_functions.win_readonly_directories_force_remove(dir)
        except Exception as exp:
            # Could be files we try to remove, which will throw exceptions
            pass



