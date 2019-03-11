import jenkins
from dataverk_cli.dataverk_base import DataverkBase
from dataverk_cli.deploy.deploy_connector import DeployConnector
from dataverk_cli.deploy.deploy_key import DeployKey
from dataverk_cli.deploy import deploy_config
from dataverk_cli.cli.cli_utils import repo_info
from collections.abc import Mapping


class DataverkSchedule(DataverkBase):
    def __init__(self, settings: Mapping, envs: Mapping):
        super().__init__(settings=settings, envs=envs)

        remote_repo_url = repo_info.get_remote_url()
        deploy_key = DeployKey(settings, envs, remote_repo_url)
        self._edit_config_files(remote_repo_url, deploy_key)
        jenkins_server = jenkins.Jenkins(url=self._settings_store["jenkins"]["url"],
                                         username=envs['USER_IDENT'],
                                         password=envs['PASSWORD'])

        self._deploy_conn = DeployConnector(job_name=settings["package_name"], build_server=jenkins_server)

    def run(self):
        try:
            self._deploy_conn.configure_job()
        except Exception:
            raise Exception(f'Unable to setup pipeline for datapackage {self._settings_store["package_name"]}')

        print(f'Job for datapackage {self._settings_store["package_name"]} is set up/reconfigured. '
              f'To complete the pipeline setup all local changes must be pushed to remote repository')

    def _edit_config_files(self, remote_repo_url, deploy_key: DeployKey):
        deploy_config.edit_service_user_config(settings_store=self._settings_store)
        deploy_config.edit_cronjob_config(settings_store=self._settings_store)
        deploy_config.edit_jenkins_job_config(remote_repo_url=remote_repo_url, credential_id=deploy_key.name)
