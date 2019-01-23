""" Handles the init command"""

import argparse
from dataverk_cli.cli.cli_utils import env_store_functions



def handle(args):

    envs = env_store_functions.get_env_store()


    settings = create_settings_dict(args=args, envs=envs)

    init = get_datapackage_object(action=Action.INIT, settings=settings, envs=envs)
    if user_input.cli_question(f'Vil du opprette datapakken ({settings["package_name"]})? [j/n] '):
        init.run()
    else:
        print(f'Datapakken {settings["package_name"]} ble ikke opprettet')





def create_env_store(args):

    # Is project internal
    if args.nav_internal is True:
        envs = env_store_functions.get_env_store()
    else:
        envs = {}


