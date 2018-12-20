import os
import json

from shutil import rmtree
from distutils.dir_util import copy_tree
from . import settings_loader

from .dataverk_base import DataverkBase
from dataverk.context.env_store import EnvStore


class DataverkInit(DataverkBase):
    ''' Klasse for å opprette ny datapakke lokalt i et eksisterende repository for datapakker/datasett
    '''

    def __init__(self, settings: dict, envs: EnvStore):
        super().__init__(settings=settings, envs=envs)


    def run(self):
        ''' Entrypoint for dataverk create
        '''

        if self._folder_exists_in_repo(self.settings["package_name"]):
            raise NameError(f'En mappe med navn {self.settings["package_name"]} '
                            f'eksisterer allerede i repo {self.github_project}')

        res = input(f'Vil du opprette datapakken ({self.settings["package_name"]}) i {self.github_project}? [j/n] ')

        if res in {'j', 'ja', 'y', 'yes'}:
            try:
                self._create()
            except Exception:
                if os.path.exists(self.settings["package_name"]):
                    rmtree(self.settings["package_name"])
                raise Exception(f'Klarte ikke generere datapakken {self.settings["package_name"]}')
        else:
            print(f'Datapakken {self.settings["package_name"]} ble ikke opprettet')

    def _create(self):
        ''' Oppretter ny datapakke med ønsket konfigurasjon
        '''

        self._create_datapackage_local()
        self._write_settings_file(path=self.settings["package_name"])
        self._edit_package_metadata()

        print(f'Datapakken {self.settings["package_name"]} er opprettet')

    def _create_datapackage_local(self):
        ''' Lager mappestrukturen for datapakken lokalt og henter template filer
        '''

        os.mkdir(self.settings["package_name"])

        templates_path = ""
        try:
            templates_loader = settings_loader.GitSettingsLoader(url=self.envs["TEMPLATES_REPO"])
            templates_path = templates_loader.download_to(".")
            copy_tree(os.path.join(str(templates_path), 'file_templates'), self.settings["package_name"])
        except OSError:
            raise OSError(f'Templates mappe eksisterer ikke.')
        finally:
            if os.path.exists(str(templates_path)):
                rmtree(str(templates_path))

    def _edit_package_metadata(self):
        '''  Tilpasser metadata fil til datapakken
        '''

        try:
            with open(os.path.join(self.settings["package_name"], 'METADATA.json'), 'r') as metadatafile:
                package_metadata = json.load(metadatafile)
        except OSError:
            raise OSError(f'Finner ikke METADATA.json fil')

        package_metadata['Datapakke_navn'] = self.settings["package_name"]
        package_metadata['Bucket_navn'] = 'nav-opendata'

        try:
            with open(os.path.join(self.settings["package_name"], 'METADATA.json'), 'w') as metadatafile:
                json.dump(package_metadata, metadatafile, indent=2)
        except OSError:
            raise OSError(f'Finner ikke METADATA.json fil')

    def _write_settings_file(self, path: str):
        try:
            with open(os.path.join(path, 'settings.json'), 'w') as settings_file:
                json.dump(self.settings, settings_file, indent=2)
        except OSError:
            raise OSError(f'Klarte ikke å skrive settings fil for datapakke')



