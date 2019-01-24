import json
from dataverk_cli.cli.cli_utils import settings_loader
from importlib_resources import read_text
from collections.abc import Mapping
from pathlib import Path


def create_settings_dict(args, env_store: Mapping):
    """ Create dict containing settings data for dataverk based on if the project should be initialized as a internal
    or open project.

    :return dict containing settings data

    """
    if is_internal_project(args):
        try:
            resource_url = env_store["SETTINGS_REPO"]
        except KeyError:
            raise KeyError(f"env_store({env_store}) has to contain a SETTINGS_REPO"
                           f" variable to initialize internal project ")

        settings_dict = settings_loader.load_settings_file_from_resource(resource_url)
        settings_dict["internal"] = "true"
    else:
        settings_dict = json.loads(read_text(package='dataverk_cli.templates',
                                             resource='settings.json'))

    return settings_dict


def is_internal_project(args):
    """
    :param args: commandline arguments
    :return: is the project flagged as an internal project
    """

    if args.internal:
        return True
    else:
        return False


def get_settings_dict(path: Path=Path('settings.json')):
    """ Create dict containing settings data from datapackage settings.json file

    :param path: Path to settings.json file
    :return: settings dict
    """

    try:
        with path.open('r') as settingsfile:
            return json.load(settingsfile)
    except OSError:
        raise OSError(f'Settings file missing in datapackage')
