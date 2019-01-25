from dataverk_cli.dataverk_base import DataverkBase
from dataverk_cli.scheduling import scheduler_factory
from collections.abc import Mapping


class DataverkSchedule(DataverkBase):
    def __init__(self, settings: Mapping, envs: Mapping):
        super().__init__(settings=settings, envs=envs)

        self._scheduler = scheduler_factory.create_scheduler(settings_store=settings, env_store=envs)

    def run(self):

        try:
            self._schedule_job()
        except Exception:
            raise Exception(f'Klarte ikke sette opp pipeline for datapakke {self._settings_store["package_name"]}')

        print(f'Jobb for datapakke {self._settings_store["package_name"]} er satt opp/rekonfigurert. '
              f'For å fullføre oppsett av pipeline må endringer pushes til remote repository')

    def _schedule_job(self):
        ''' Setter opp schedulering av job for datapakken
        '''

        self._scheduler.configure_job()
