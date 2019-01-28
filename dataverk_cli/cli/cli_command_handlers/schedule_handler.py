""" Handles the schedule command"""

import subprocess
from dataverk.utils.validators import validate_cronjob_schedule
from dataverk_cli.cli.cli_utils import user_input
from collections.abc import MutableMapping


def handle(args, settings_dict: MutableMapping) -> MutableMapping:
    """
    Handles the schedule command case. Configures the settings mapping object accordingly to requirements for
    scheduling of a datapackage project in a datapipeline

    :param args: command line argument object
    :return: settings_dict: Mapping, env_store: Mapping
    """

    if not datapackage_exists_in_remote_repo():
        raise FileNotFoundError(f'The datapackage must exist in a remote repository before <dataverk-cli schedule> '
                                f'can be executed. Create a remote repository and run git add->commit->push and retry.')

    update_schedule = _create_update_schedule(args)

    settings_dict["update_schedule"] = update_schedule

    if user_input.cli_question('Do you want to set up pipeline for the datapackage? [y/n] '):
        return settings_dict
    else:
        KeyboardInterrupt(f'Pipeline for datapackage {settings_dict["package_name"]} was NOT created')


def datapackage_exists_in_remote_repo():
    try:
        subprocess.check_output(["git", "cat-file", "-e", f'origin/master:Jenkinsfile'])
        return True
    except subprocess.CalledProcessError:
        return False


def _create_update_schedule(args):
    if args.update_schedule is None:
        update_schedule = input("Enter desired update schedule for datapackage "
                                "(format: \"<minute> <hour> <day of month> <month> <weekday>\", "
                                "e.g. \"0 12 * * 2,4\" gives <Every Tuesday and Thursday at 12.00 UTC>): ")
        if not update_schedule:
            update_schedule = "* * 31 2 *"  # Default value Feb 31 (i.e. never)
    else:
        update_schedule = args.update_schedule

    validate_cronjob_schedule(update_schedule)

    return update_schedule
