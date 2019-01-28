""" Handles the delete command"""

import argparse
from dataverk_cli.cli.cli_utils import env_store_functions
from dataverk_cli.cli.cli_utils import setting_store_functions
from dataverk_cli.cli.cli_utils import user_input
from dataverk_cli.dataverk_factory import get_datapackage_object
from dataverk_cli.dataverk_base import Action
from collections.abc import Mapping


def handle(args):
    """
    Handles the delete command case. Configures the settings mapping object accordingly to requirements for deletion
    of a datapackage project

    :param args: command line argument object
    :return: settings_store: Mapping, env_store: Mapping
    """


    settings_dict = setting_store_functions.get_settings_dict()
    env_store = get_env_store(settings=settings_dict)


    if user_input.cli_question(f'Er du sikker på at du ønsker å fjerne datapakken {settings_dict["package_name"]}? [j/n] '):
        return settings_dict, env_store
    else:
        print(f'Datapakken {settings_dict["package_name"]} ble ikke fjernet')



