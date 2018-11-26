import jenkins
import os

from dataverk.utils.env_store import EnvStore
from dataverk.utils import resource_discoverer
from . import settings_loader, settings_creator
from .datapackage_create import CreateDataPackage
from shutil import rmtree
from abc import ABC
from enum import Enum
from pathlib import Path


class Action(Enum):
    CREATE = 1
    UPDATE = 2
    DELETE = 3


class DataPackage(ABC):
    ''' Abstrakt baseklasse for dataverk scripts.
    '''

    def __init__(self, settings: dict, envs: EnvStore):
        self._verify_class_init_arguments(settings, envs)

        self.settings = settings
        self.github_project = self._get_github_url()
        self.envs = envs

        if not self._is_in_repo_root():
            raise Exception(f'dataverk create/update/delete må kjøres fra topp-nivået i git repoet')

        self.jenkins_server = jenkins.Jenkins(self.settings["jenkins"]["url"],
                                              username=self.envs['USER_IDENT'],
                                              password=self.envs['PASSWORD'])

    def _verify_class_init_arguments(self, settings, envs):
        if not isinstance(settings, dict):
            raise TypeError(f'settings parameter must be of type dict')

        if not isinstance(envs, EnvStore):
            raise TypeError(f'envs parameter must be of type EnvStore')

    def _is_in_repo_root(self):
        ''' Sjekk på om create_dataverk kjøres fra toppnivå i repo.

        :return: boolean: "True" hvis dataverk_create kjøres fra toppnivå i repo, "False" ellers
        '''

        current_dir = os.getcwd()
        git_root = os.popen('git rev-parse --show-toplevel').read().strip()

        return os.path.samefile(current_dir, git_root)

    def _folder_exists_in_repo(self, name: str):
        ''' Sjekk på om det finnes en mappe i repoet med samme navn som ønsket pakkenavn

        :return: boolean: "True" hvis pakkenavn allerede er tatt i bruk, "False" ellers
        '''

        for filename in os.listdir(os.getcwd()):
            if name == filename:
                return True

        return False

    def _print_datapackage_config(self):
        print("\n-------------Datapakke------------------------" +
              "\nDatapakkenavn: " + self.settings["package_name"] +
              "\ngithub repo: " + self.github_project +
              "\ncronjob schedule: " + self.settings["update_schedule"] +
              "\nNAIS namespace: " + self.settings["nais_namespace"] +
              "\n-------------------------------------------------\n")

    def _get_github_url(self):
        if not os.popen('git rev-parse --is-inside-work-tree').read().strip():
            raise Exception("dataverk create må kjøres fra et git repository")

        return os.popen('git config --get remote.origin.url').read().strip()


def get_datapackage_object(action: Action, args) -> type(DataPackage):
    resource_files = resource_discoverer.search_for_files(start_path=Path('.'), file_names=('.env',), levels=3)

    if '.env' not in resource_files:
        Exception(f'.env fil må finnes i repo for å kunne kjøre dataverk create')

    envs = EnvStore(path=Path(resource_files['.env']))

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

    if action is Action.CREATE:
        return CreateDataPackage(settings=settings, envs=envs)
    elif action is Action.UPDATE:
        pass
    elif action is Action.DELETE:
        pass
    else:
        raise Exception(f'Invalid script parameter "action={action}" for get_datapackage_object()')
