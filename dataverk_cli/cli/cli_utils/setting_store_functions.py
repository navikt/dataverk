from dataverk.context import EnvStore
import json
from dataverk_cli.cli.cli_utils import settings_loader
from importlib_resources import read_text
from collections.abc import Mapping


def create_settings_dict(args, env_store: Mapping):
    """ Create dict containing settings data for dataverk based on if the project should be initialized as a internal
    or open project.

    :return dict containing settings data

    """
    if is_intenal_project(args):
        try:
            resource_url = env_store["SETTINGS_REPO"]
        except KeyError:
            raise KeyError(f"env_store({env_store}) has to contain a SETTINGS_REPO"
                           f" variable to initialize internal project ")

        settings_dict = settings_loader.load_settings_file_from_resource(resource_url)

    else:
        settings_dict = json.loads(read_text(package='dataverk_cli.templates',
                                        resource='settings.json'))




    return settings_dict


def is_intenal_project(args):
    """
    :param args: commandline arguments
    :return: is the project flagged as an internal project
    """

    if args.internal:
        return True
    else:
        return False


