""" Handles the delete command"""

import argparse
from dataverk_cli.cli.cli_utils import env_store_functions
from dataverk_cli.cli.cli_utils import setting_store_functions
from dataverk_cli.cli.cli_utils import user_input
from dataverk_cli.dataverk_factory import get_datapackage_object
from dataverk_cli.dataverk_base import Action
from collections.abc import Mapping


def handle(args):

    settings_dict = setting_store_functions.get_settings_dict()
    envs = get_env_store(settings=settings_dict)

    delete = get_datapackage_object(action=Action.DELETE, settings=settings_dict, envs=envs)

    if user_input.cli_question(f'Er du sikker på at du ønsker å fjerne datapakken {settings_dict["package_name"]}? [j/n] '):
        delete.run()
    else:
        print(f'Datapakken {settings_dict["package_name"]} ble ikke fjernet')


def get_env_store(settings: Mapping):
    # Is project internal
    if "internal" in settings:
        return env_store_functions.get_env_store()
    else:
        return {}


