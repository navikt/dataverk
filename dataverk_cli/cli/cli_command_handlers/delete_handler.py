""" Handles the delete command"""

from dataverk_cli.cli.cli_utils import user_input
from collections.abc import MutableMapping


def handle(args, settings_dict: MutableMapping) -> MutableMapping:
    """
    Handles the delete command case. Configures the settings mapping object accordingly to requirements for deletion
    of a datapackage project

    :param args: command line argument object
    :return: settings_store: Mapping, env_store: Mapping
    """

    if user_input.cli_question(f'Are you sure you want to remove datapackage {settings_dict["package_name"]}? [y/n] '):
        return settings_dict
    else:
        raise KeyboardInterrupt(f'Datapackage {settings_dict["package_name"]} was NOT removed')
