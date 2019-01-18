from dataverk.context.env_store import EnvStore
from dataverk.context.settings import SettingsStore
from .dataverk_init import DataverkInit
from .dataverk_schedule import DataverkSchedule
from .dataverk_delete import DataverkDelete
from .dataverk_base import Action
from .dataverk_factory import get_datapackage_object
from .dataverk_base import create_settings_dict, get_settings_dict
from .cli_utils import user_input
from dataverk.utils import resource_discoverer
from pathlib import Path
from dataverk.utils.validators import validate_cronjob_schedule


class DataverkHandlerWrapper:

    def __init__(self, args):
        self._args = args
        resource_files = resource_discoverer.search_for_files(start_path=Path('.'), file_names=('.env',), levels=3)
        if '.env' not in resource_files:
            raise FileNotFoundError(f'.env fil må finnes i repo for å kunne kjøre dataverk-cli init/schedule/delete')

        self._envs = EnvStore(path=Path(resource_files['.env']))

    def init(self):
        settings = create_settings_dict(args=self._args, envs=self._envs)
        init = get_datapackage_object(action=Action.INIT, settings=settings, envs=self._envs)
        if user_input.cli_question(f'Vil du opprette datapakken ({settings["package_name"]})? [j/n] '):
            init.run()
        else:
            print(f'Datapakken {self._package_name} ble ikke opprettet')

    def schedule(self):
        settings = get_settings_dict(package_name=self._args.package_name)

        if self._args.package_name is None:
            raise ValueError(f'For å kjøre <dataverk-cli schedule> må pakkenavn angis (-p, --package-name). '
                             f'F.eks. <dataverk-cli_utils schedule --package-name min-pakke')

        if self._args.update_schedule is None:
            update_schedule = input("Skriv inn ønsket oppdateringsschedule for datapakken "
                                    "(format: \"<minutt> <time> <dag i måned> <måned> <ukedag>\", "
                                    "f.eks. \"0 12 * * 2,4\" vil gi <Hver tirsdag og torsdag kl 12.00 UTC>): ")
            if not update_schedule:
                update_schedule = "* * 31 2 *"  # Default value Feb 31 (i.e. never)
        else:
            update_schedule = self._args.update_schedule

        validate_cronjob_schedule(update_schedule)
        settings["update_schedule"] = update_schedule

        schedule = get_datapackage_object(action=Action.SCHEDULE, settings=settings, envs=self._envs)

        if user_input.cli_question('Vil du sette opp pipeline for datapakken over? [j/n] '):
            schedule.run()
        else:
            print(f'Pipeline for datapakke {self._package_name} ble ikke opprettet')

    def delete(self):
        pass