import os
from abc import ABC, abstractmethod
from enum import Enum
from shutil import rmtree
from collections.abc import Mapping


class Action(Enum):
    INIT = 1
    SCHEDULE = 2
    DELETE = 3


CONFIG_FILE_TYPES = ('.json', '.md', '.ipynb')

PACKAGE_FILES = ('.dockerignore', 'cronjob.yaml', 'dockerEntryPoint.sh', 'Dockerfile', 'etl.ipynb',
                 'jenkins_config.xml', 'Jenkinsfile', 'LICENSE.md', 'METADATA.json', 'README.md',
                 'requirements.txt', 'settings.json', 'etl.py', 'datapackage.json')

PACKAGE_FOLDERS = ('sql', 'resources')


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
                self._remove_folder_structure(folder)
            except OSError:
                pass

    def _remove_folder_structure(self, path: str):
        rmtree(path=path, onerror=self._delete_rw_windows)

    def _delete_rw_windows(self, action, name, exc):
        os.chmod(name, 128)
        os.remove(name)

    def _folder_exists_in_repo(self, name: str):
        ''' Sjekk på om det finnes en mappe i repoet med samme navn som ønsket pakkenavn

        :return: boolean: "True" hvis pakkenavn allerede er tatt i bruk, "False" ellers
        '''

        for filename in os.listdir(os.getcwd()):
            if name == filename:
                return True

        return False

    @abstractmethod
    def run(self):
        raise NotImplementedError()
