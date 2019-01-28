""" Handles the init command"""

from dataverk.utils.validators import validate_datapackage_name
from dataverk_cli.cli.cli_utils import user_input

from collections.abc import MutableMapping


def handle(args, settings_store: MutableMapping) -> MutableMapping:
    """
    Handles the init command case. Configures the settings mapping object accordingly to requirements for initialization
    of a new datapackage project

    :param args: command line argument object
    :param settings_store:
    :param env_store:
    :return: settings_store: Mapping, env_store: Mappin
    """

    if args.package_name is None:
        settings_store["package_name"] = user_input.prompt_for_user_input(arg="pakkenavn")
    else:
        settings_store["package_name"] = args.package_name

    validate_datapackage_name(settings_store["package_name"])

    if user_input.cli_question(f'Do you want to create the datapackage ({settings_store["package_name"]})? [y/n] '):
        return settings_store
    else:
        raise KeyboardInterrupt(f'Datapakken {settings_store["package_name"]} was not created')
