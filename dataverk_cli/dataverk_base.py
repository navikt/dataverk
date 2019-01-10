import os
import json

from dataverk_cli.cli_utils import settings_creator, settings_loader
from dataverk.context.env_store import EnvStore
from abc import ABC, abstractmethod
from enum import Enum
from shutil import rmtree
from collections.abc import Mapping
from pathlib import Path


class Action(Enum):
    INIT = 1
    SCHEDULE = 2
    DELETE = 3


class BucketStorage(Enum):
    GITHUB = "Github"
    DATAVERK_S3 = "Dataverk_S3"


class DataverkBase(ABC):
    ''' Abstrakt baseklasse for dataverk scripts.
    '''

    def __init__(self, settings: Mapping, envs: Mapping):
        self._verify_class_init_arguments(settings, envs)

        self.settings = settings
        self.github_project = self._get_github_url()
        self.github_project_ssh = self._get_ssh_url()
        self.envs = envs

    def _verify_class_init_arguments(self, settings, envs):
        if not isinstance(settings, dict):
            raise TypeError(f'settings parameter must be of type dict')

        if not isinstance(envs, EnvStore):
            raise TypeError(f'envs parameter must be of type EnvStore')

    def _folder_exists_in_repo(self, name: str):
        ''' Sjekk på om det finnes en mappe i repoet med samme navn som ønsket pakkenavn

        :return: boolean: "True" hvis pakkenavn allerede er tatt i bruk, "False" ellers
        '''

        for filename in os.listdir(os.getcwd()):
            if name == filename:
                return True

        return False

    def _print_datapipeline_config(self):
        print("\n-------------Datapakke-----------------------------" +
              "\nDatapakkenavn: " + self.settings["package_name"] +
              "\ngithub repo: " + self.github_project +
              "\ncronjob schedule: " + self.settings["update_schedule"] +
              "\nNAIS namespace: " + self.settings["nais_namespace"] +
              "\n-------------------------------------------------\n")

    def _get_github_url(self):
        return os.popen('git config --get remote.origin.url').read().strip()

    def _get_ssh_url(self):
        url_list = Path(self.github_project).parts
        org_name = url_list[2]
        repo_name = url_list[3]
        return f'git@github.com:{org_name}/{repo_name}'

    @abstractmethod
    def run(self):
        raise NotImplementedError()


def create_settings_dict(args, envs: EnvStore):
    default_settings_path = ""
    try:
        default_settings_loader = settings_loader.GitSettingsLoader(url=envs["SETTINGS_REPO"])
        default_settings_path = default_settings_loader.download_to('.')

        settings_creator_object = settings_creator.get_settings_creator(args=args,
                                                                        default_settings_path=str(default_settings_path))
        settings = settings_creator_object.create_settings()
    finally:
        if os.path.exists(str(default_settings_path)):
            rmtree(str(default_settings_path))

    return settings


def get_settings_dict(package_name):
    try:
        with open(os.path.join(package_name, "settings.json"), 'r') as settings_file:
            settings = json.load(settings_file)
    except OSError:
        raise OSError(f'Settings file missing in datapackage {package_name}')
    return settings
