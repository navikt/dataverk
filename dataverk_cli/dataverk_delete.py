from .dataverk_base import DataverkBase
from dataverk_cli.scheduling import scheduler_factory
from collections.abc import Mapping


class DataverkDelete(DataverkBase):

    def __init__(self, settings: Mapping, envs: Mapping):
        super().__init__(settings=settings, envs=envs)

        try:
            self._scheduler = scheduler_factory.create_scheduler(settings_store=settings, env_store=envs)
        except LookupError:
            self._scheduler = None

    def run(self):
        ''' Entrypoint for dataverk delete
        '''

        self._delete()
        print(f'Datapakken {self._settings_store["package_name"]} er fjernet')

    def _delete(self):
        ''' Fjerner datapakken og jenkinsjobben
        '''

        if self._scheduler is not None:
            if self._scheduler.job_exist():
                self._scheduler.delete_job()
            else:
                print(f'Det finnes ingen jobber med navn {self._settings_store["package_name"]}.')

        self._clean_up_files()
