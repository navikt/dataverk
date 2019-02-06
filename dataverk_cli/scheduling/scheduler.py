from abc import ABC, abstractmethod
from collections import Mapping
from dataverk_cli.cli.cli_utils import repo_info


class Scheduler(ABC):
    """ Abstrakt base klasse for CI jobb skedulerings tjenester

        Definerer en kontrakt for alle CI jobb skedulerings tjeneste wrappers.
    """

    def __init__(self, settings_store: Mapping, env_store: Mapping, repo_path: str="."):
        self._env_store = env_store
        self._settings_store = settings_store
        self._github_project = repo_info.get_remote_url(repo_path=repo_path)
        self._github_project_ssh = repo_info.convert_to_ssh_url(self._github_project)

    @abstractmethod
    def configure_job(self):
        raise NotImplementedError()

    @abstractmethod
    def delete_job(self):
        raise NotImplementedError()

    @abstractmethod
    def job_exist(self):
        raise NotImplementedError()
