""" Handles the init command"""

import argparse
from dataverk_cli.cli.cli_utils import env_store_functions
from dataverk_cli.cli.cli_utils import setting_store_functions
from dataverk.utils.validators import validate_datapackage_name
from dataverk_cli.cli.cli_utils import user_input
from dataverk_cli.dataverk_factory import get_datapackage_object
from dataverk_cli.dataverk_base import Action


def handle(args):

    envs = create_env_store(args)
    settings_dict = setting_store_functions.create_settings_dict(args=args, env_store=envs)

    init = get_datapackage_object(action=Action.INIT, settings=settings_dict, envs=envs)

    if args.package_name is None:
        settings_dict["package_name"] = user_input.prompt_for_user_input(arg="pakkenavn")
    else:
        settings_dict["package_name"] = args.package_name

    validate_datapackage_name(settings_dict["package_name"])

    if user_input.cli_question(f'Vil du opprette datapakken ({settings_dict["package_name"]})? [j/n] '):
        init.run()
    else:
        print(f'Datapakken {settings_dict["package_name"]} ble ikke opprettet')


def create_env_store(args):
    # Is project internal
    if args.internal is True:
        return env_store_functions.get_env_store()
    else:
        return {}


