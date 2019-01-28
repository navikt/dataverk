from dataverk_cli.cli.cli_utils import setting_store_functions
from dataverk_cli.cli.cli_utils import env_store_functions
from collections.abc import MutableMapping, Mapping


def get_package_configuration(args, initialize: bool=False) -> (MutableMapping, Mapping):
    ''' Create setting and env stores for command handling

    :param args: optional command line arguments
    :param initialize: Boolean parameter indicating whether the settings_dict shall be created or read from local settings.json file
    :return: settings_dict, env_store
    '''

    env_store = env_store_functions.safe_create_env_store(args)

    if initialize:
        settings_dict = setting_store_functions.create_settings_dict(args=args, env_store=env_store)
    else:
        settings_dict = setting_store_functions.get_settings_dict()

    return settings_dict, env_store
