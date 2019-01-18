import os
from abc import ABC, abstractmethod
from collections import Mapping
from pathlib import Path


class Scheduler(ABC):
    """ Abstrakt base klasse for CI jobb skedulerings tjenester

        Definerer en kontrakt for alle CI jobb skedulerings tjeneste wrappers.
    """

    def __init__(self, settings_store: Mapping, env_store: Mapping):
        self._env_store = env_store
        self._settings_store = settings_store
        self._github_project = self._get_github_url()
        self._github_project_ssh = self._get_ssh_url()

    @abstractmethod
    def configure_job(self, job_name, config_file):
        raise NotImplementedError()

    @abstractmethod
    def delete_job(self, job_name):
        raise NotImplementedError()

    @abstractmethod
    def job_exist(self, job_name):
        raise NotImplementedError()

    def _get_github_url(self):
        return os.popen('git config --get remote.origin.url').read().strip()

    def _get_ssh_url(self):
        url_list = Path(self._github_project).parts
        org_name = url_list[2]
        repo_name = url_list[3]
        return f'git@github.com:{org_name}/{repo_name}'
