from dataverk_cli.dataverk_base import DataverkBase
from dataverk_cli.scheduling import scheduler_factory
from dataverk_cli.scheduling.deploy_key import DeployKey
from dataverk_cli.cli.cli_utils import repo_info
from collections.abc import Mapping


class DataverkSchedule(DataverkBase):
    def __init__(self, settings: Mapping, envs: Mapping):
        super().__init__(settings=settings, envs=envs)

        remote_repo_url = repo_info.get_remote_url()
        deploy_key = DeployKey(settings, envs, remote_repo_url)

        self._scheduler = scheduler_factory.create_scheduler(settings_store=settings,
                                                             env_store=envs,
                                                             remote_repo_url=remote_repo_url,
                                                             deploy_key=deploy_key)

    def run(self):
        try:
            self._scheduler.configure_job()
        except Exception:
            raise Exception(f'Unable to setup pipeline for datapackage {self._settings_store["package_name"]}')

        print(f'Job for datapackage {self._settings_store["package_name"]} is set up/reconfigured. '
              f'To complete the pipeline setup all local changes must be pushed to remote repository')
