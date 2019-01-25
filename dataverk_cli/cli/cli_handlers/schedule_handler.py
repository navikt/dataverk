""" Handles the schedule command"""

import argparse
import subprocess
from dataverk_cli.cli.cli_utils import env_store_functions
from dataverk_cli.cli.cli_utils import setting_store_functions
from dataverk.utils.validators import validate_cronjob_schedule
from dataverk_cli.cli.cli_utils import user_input
from dataverk_cli.dataverk_factory import get_datapackage_object
from dataverk_cli.dataverk_base import Action
from collections.abc import Mapping


def handle(args) -> (Mapping, Mapping):
    """
    Handles the schedule command case. Configures the settings mapping object accordingly to requirements for
    scheduling of a datapackage project in a datapipeline

    :param args: command line argument object
    :return: settings_store: Mapping, env_store: Mapping
    """

    if not datapackage_exists_in_remote_repo():
        raise FileNotFoundError(f'Datapakken må eksistere i remote repositoriet før man kan eksekvere '
                                f'<dataverk-cli schedule>. git add->commit->push av datapakken og prøv på nytt.')

    settings_dict = setting_store_functions.get_settings_dict()
    env_store = get_env_store(settings=settings_dict)

    update_schedule = _create_update_schedule(args)

    validate_cronjob_schedule(update_schedule)
    settings_dict["update_schedule"] = update_schedule

    if user_input.cli_question('Vil du sette opp pipeline for datapakken? [j/n] '):
        return settings_dict, env_store
    else:
        KeyboardInterrupt(f'Pipeline for datapakke {settings_dict["package_name"]} ble ikke opprettet')


def get_env_store(settings: Mapping):
    # Is project internal
    if "internal" in settings:
        return env_store_functions.get_env_store()
    else:
        return {}


def datapackage_exists_in_remote_repo():
    try:
        subprocess.check_output(["git", "cat-file", "-e", f'origin/master:Jenkinsfile'])
        return True
    except subprocess.CalledProcessError:
        return False


def _create_update_schedule(args):
    if args.update_schedule is None:
        update_schedule = input("Skriv inn ønsket oppdateringsschedule for datapakken "
                                "(format: \"<minutt> <time> <dag i måned> <måned> <ukedag>\", "
                                "f.eks. \"0 12 * * 2,4\" vil gi <Hver tirsdag og torsdag kl 12.00 UTC>): ")
        if not update_schedule:
            update_schedule = "* * 31 2 *"  # Default value Feb 31 (i.e. never)
    else:
        update_schedule = args.update_schedule

    return update_schedule
