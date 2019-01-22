from dataverk.context.env_store import EnvStore
from .dataverk_base import Action
from .dataverk_factory import get_datapackage_object
from .dataverk_base import create_settings_dict, get_settings_dict
from .cli_utils import user_input
from dataverk.utils import resource_discoverer
from pathlib import Path
from dataverk.utils.validators import validate_cronjob_schedule
from collections.abc import Mapping


def _get_env_store():
    resource_files = resource_discoverer.search_for_files(start_path=Path('.'), file_names=('.env',), levels=3)
    if '.env' not in resource_files:
        raise FileNotFoundError(f'.env fil må finnes i repo for å kunne kjøre dataverk-cli init/schedule/delete')

    return EnvStore(path=Path(resource_files['.env']))


def init_wrapper(args):
    if args.nav_internal is True:
        envs = _get_env_store()
    else:
        envs = {}

    settings = create_settings_dict(args=args, envs=envs)

    init = get_datapackage_object(action=Action.INIT, settings=settings, envs=envs)
    if user_input.cli_question(f'Vil du opprette datapakken ({settings["package_name"]})? [j/n] '):
        init.run()
    else:
        print(f'Datapakken {settings["package_name"]} ble ikke opprettet')


def schedule_wrapper(args):
    settings = get_settings_dict()
    if settings.get("nav_internal", "") == "true":
        envs = _get_env_store()
    else:
        envs = {}

    if args.package_name is None:
        raise ValueError(f'For å kjøre <dataverk-cli schedule> må pakkenavn angis (-p, --package-name). '
                         f'F.eks. <dataverk-cli_utils schedule --package-name min-pakke')

    if args.update_schedule is None:
        update_schedule = input("Skriv inn ønsket oppdateringsschedule for datapakken "
                                "(format: \"<minutt> <time> <dag i måned> <måned> <ukedag>\", "
                                "f.eks. \"0 12 * * 2,4\" vil gi <Hver tirsdag og torsdag kl 12.00 UTC>): ")
        if not update_schedule:
            update_schedule = "* * 31 2 *"  # Default value Feb 31 (i.e. never)
    else:
        update_schedule = args.update_schedule

    validate_cronjob_schedule(update_schedule)
    settings["update_schedule"] = update_schedule

    schedule = get_datapackage_object(action=Action.SCHEDULE, settings=settings, envs=envs)

    if user_input.cli_question('Vil du sette opp pipeline for datapakken over? [j/n] '):
        schedule.run()
    else:
        print(f'Pipeline for datapakke {settings["package_name"]} ble ikke opprettet')


def delete_wrapper(args):
    settings = get_settings_dict()
    if settings.get("nav_internal", "").lower() == "true":
        envs = _get_env_store()
    else:
        envs = {}

    if settings["package_name"] is None:
        raise ValueError(f'For å kjøre <dataverk-cli delete> må pakkenavn angis (-p, --package-name). '
                         f'F.eks. <dataverk-cli delete --package-name min-pakke')

    print(f'Fjerning av datapakke {settings["package_name"]}')

    delete = get_datapackage_object(action=Action.DELETE, settings=settings, envs=envs)

    if user_input.cli_question(f'Er du sikker på at du ønsker å fjerne datapakken {settings["package_name"]}? [j/n] '):
        delete.run()
    else:
        print(f'Datapakken {settings["package_name"]} ble ikke fjernet')