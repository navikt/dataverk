import os
import json
from dataverk.utils.validators import validate_datapackage_name
from dataverk_cli.cli_utils import settings_creator, settings_loader
from dataverk_cli.cli_utils import user_input
from dataverk.context.env_store import EnvStore
from abc import ABC, abstractmethod
from enum import Enum
from shutil import rmtree
from collections.abc import Mapping
from importlib_resources import read_text


class Action(Enum):
    INIT = 1
    SCHEDULE = 2
    DELETE = 3


class BucketStorage(Enum):
    GITHUB = "Github"
    DATAVERK_S3 = "Dataverk_S3"


CONFIG_FILE_TYPES = ('.json', '.md', '.ipynb')

PACKAGE_FILES = ('.dockerignore', 'cronjob.yaml', 'dockerEntryPoint.sh', 'Dockerfile', 'etl.ipynb',
                 'jenkins_config.xml', 'Jenkinsfile', 'LICENSE.md', 'METADATA.json', 'README.md',
                 'requirements.txt', 'settings.json', '.travis.yml')

PACKAGE_FOLDERS = ('sql', '.circleci')


class DataverkBase(ABC):
    ''' Abstrakt baseklasse for dataverk scripts.
    '''

    def __init__(self, settings: Mapping, envs: Mapping):
        self._verify_class_init_arguments(settings, envs)

        self._settings_store = settings
        self._envs = envs

    def _verify_class_init_arguments(self, settings, envs):
        if not isinstance(settings, Mapping):
            raise TypeError(f'settings parameter must be of type Mapping')

        if not isinstance(envs, Mapping):
            raise TypeError(f'envs parameter must be of type Mapping')

    def _clean_up_files(self):
        ''' Fjern alle filer som tilhører pakken

        :return:
        '''
        for file in PACKAGE_FILES:
            try:
                os.remove(file)
            except OSError:
                pass

        for folder in PACKAGE_FOLDERS:
            try:
                remove_folder_structure(folder)
            except OSError:
                pass

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
              "\nDatapakkenavn: " + self._settings_store["package_name"] +
              "\ngithub repo: " + self.github_project +
              "\ncronjob schedule: " + self._settings_store["update_schedule"] +
              "\nNAIS namespace: " + self._settings_store["nais_namespace"] +
              "\n-------------------------------------------------\n")

    @abstractmethod
    def run(self):
        raise NotImplementedError()


def remove_folder_structure(path: str):
    rmtree(path=path, onerror=delete_rw_windows)


def delete_rw_windows(action, name, exc):
    os.chmod(name, 128)
    os.remove(name)


def create_settings_dict(args, envs: EnvStore):
    if args.nav_internal:
        default_settings_path = ""
        try:
            default_settings_loader = settings_loader.GitSettingsLoader(url=envs["SETTINGS_REPO"])
            default_settings_path = default_settings_loader.download_to('.')

            settings_creator_object = settings_creator.get_settings_creator(args=args,
                                                                            default_settings_path=str(default_settings_path))
            settings = settings_creator_object.create_settings()
            settings["nav_internal"] = "true"
        finally:
            if os.path.exists(str(default_settings_path)):
                remove_folder_structure(str(default_settings_path))
    else:
        settings = json.loads(read_text(package='dataverk_cli.templates',
                                        resource='settings.json'))

    if args.package_name is None:
        settings["package_name"] = user_input.prompt_for_user_input(arg="pakkenavn")
    else:
        settings["package_name"] = args.package_name

    validate_datapackage_name(settings["package_name"])

    return settings


def get_settings_dict():
    try:
        with open("settings.json", 'r') as settings_file:
            settings = json.load(settings_file)
    except OSError:
        raise OSError(f'Settings file missing in datapackage')
    return settings
