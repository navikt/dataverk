""" Handles the init command"""

import argparse
from dataverk_cli.cli.cli_utils import env_store_functions
from dataverk_cli.cli.cli_utils import setting_store_functions
from dataverk.utils.validators import validate_datapackage_name
from dataverk_cli.cli.cli_utils import user_input


def handle(args):

    envs = env_store_functions.get_env_store()

    settings_dict = setting_store_functions.create_settings_dict(args=args, env_store=envs)

    init = get_datapackage_object(action=Action.INIT, settings=settings_dict, envs=envs)

    settings_dict["package_name"] = args.package_name
    validate_datapackage_name(settings_dict["package_name"])

    if user_input.cli_question(f'Vil du opprette datapakken ({settings_dict["package_name"]})? [j/n] '):
        init.run()
    else:
        print(f'Datapakken {settings_dict["package_name"]} ble ikke opprettet')





def create_env_store(args):

    # Is project internal
    if args.nav_internal is True:
        envs = env_store_functions.get_env_store()
    else:
        envs = {}


