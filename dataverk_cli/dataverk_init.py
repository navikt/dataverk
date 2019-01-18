import os
import json
from uuid import uuid4

from shutil import rmtree
from distutils.dir_util import copy_tree
from pathlib import Path
from dataverk_cli.cli_utils import settings_loader
from .dataverk_base import DataverkBase, remove_folder_structure
from dataverk.context.env_store import EnvStore
from dataverk.context.settings import SettingsStore


class DataverkInit(DataverkBase):
    ''' Klasse for å opprette ny datapakke lokalt i et eksisterende repository for datapakker/datasett
    '''

    def __init__(self, settings: SettingsStore, envs: EnvStore):
        super().__init__(settings=settings, envs=envs)

        self._package_name = settings["package_name"]
        self._package_id = str(uuid4())

    def run(self):
        ''' Entrypoint for dataverk init
        '''

        try:
            self._create()
        except Exception:
            if os.path.exists(self._package_name):
                rmtree(self._package_name)
            raise Exception(f'Klarte ikke generere datapakken {self._package_name}')

    def _create(self):
        ''' Oppretter ny datapakke med ønsket konfigurasjon
        '''

        self._create_datapackage_local()
        self._write_settings_file()
        self._edit_package_metadata()

        print(f'Datapakken {self._package_name} er opprettet')

    def _create_datapackage_local(self):
        ''' Lager mappestrukturen for datapakken lokalt og henter template filer
        '''

        templates_path = ""
        try:
            templates_loader = settings_loader.GitSettingsLoader(url=self.envs["TEMPLATES_REPO"])
            templates_path = templates_loader.download_to(".")
            copy_tree(os.path.join(str(templates_path), 'file_templates'), self._package_name)
        except OSError:
            raise OSError(f'Templates mappe eksisterer ikke.')
        finally:
            if os.path.exists(str(templates_path)):
                remove_folder_structure(str(templates_path))

    def _write_settings_file(self):

        settings_file_path = Path(self._package_name).joinpath('settings.json')

        try:
            with settings_file_path.open('w') as settings_file:
                json.dump(self.settings, settings_file, indent=2)
        except OSError:
            raise OSError(f'Klarte ikke å skrive settings fil for datapakke')

    def _edit_package_metadata(self):
        '''  Tilpasser metadata fil til datapakken
        '''

        metadata_file_path = Path(self._package_name).joinpath("METADATA.json")

        try:
            with metadata_file_path.open('r') as metadatafile:
                package_metadata = json.load(metadatafile)
        except OSError:
            raise OSError(f'Finner ikke METADATA.json fil på Path({metadata_file_path})')

        package_metadata['datapackage_name'] = self._package_name
        package_metadata['title'] = self._package_name # default
        package_metadata['bucket_name'] = 'nav-opendata'
        package_metadata['id'] = self._package_id

        try:
            with metadata_file_path.open('w') as metadatafile:
                json.dump(package_metadata, metadatafile, indent=2)
        except OSError:
            raise OSError(f'Finner ikke METADATA.json fil på Path({metadata_file_path})')