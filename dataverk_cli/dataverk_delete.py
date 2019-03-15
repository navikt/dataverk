from .dataverk_base import DataverkBase
from dataverk_cli.deploy import deployer_factory
from collections.abc import Mapping
from dataverk_cli.cli.cli_utils.user_message_templates import WARNING_TEMPLATE


class DataverkDelete(DataverkBase):

    def __init__(self, settings: Mapping, envs: Mapping):
        super().__init__(settings=settings, envs=envs)

        try:
            self._scheduler = deployer_factory.create_deploy_connector(settings_store=settings, env_store=envs)
        except LookupError:
            self._scheduler = None

    def run(self):
        ''' Entrypoint for dataverk delete
        '''

        self._delete()
        print(f'Datapackage {self._settings_store["package_name"]} is removed')

    def _delete(self):
        ''' Fjerner datapakken og jenkinsjobben
        '''

        if self._scheduler is not None:
            try:
                self._scheduler.delete_job()
            except UserWarning as no_job_exists_warning:
                print(WARNING_TEMPLATE.format(no_job_exists_warning))

        self._clean_up_files()
