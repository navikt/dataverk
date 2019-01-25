""" Handles the init command"""

import argparse
from dataverk_cli.cli.cli_utils import env_store_functions
from dataverk_cli.cli.cli_utils import setting_store_functions
from dataverk.utils.validators import validate_datapackage_name
from dataverk_cli.cli.cli_utils import user_input
from dataverk_cli.dataverk_factory import get_datapackage_object
from dataverk_cli.dataverk_base import Action
from collections.abc import Mapping


def handle(args) -> (Mapping, Mapping):
    """
    Handles the init command case. Configures the settings mapping object accordingly to requirements for initialization
    of a new datapackage project

    :param args: command line argument object
    :return: settings_store: Mapping, env_store: Mapping
    """

    env_store = create_env_store(args)
    settings_dict = setting_store_functions.create_settings_dict(args=args, env_store=env_store)

    if args.package_name is None:
        settings_dict["package_name"] = user_input.prompt_for_user_input(arg="pakkenavn")
    else:
        settings_dict["package_name"] = args.package_name

    validate_datapackage_name(settings_dict["package_name"])

    if user_input.cli_question(f'Vil du opprette datapakken ({settings_dict["package_name"]})? [j/n] '):
        return settings_dict, env_store
    else:
        raise KeyboardInterrupt(f'Datapakken {settings_dict["package_name"]} ble ikke opprettet')



def create_env_store(args):
    # Is project internal
    if args.internal is True:
        return env_store_functions.get_env_store()
    else:
        return {}


