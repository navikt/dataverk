import os
import json

from shutil import rmtree
from distutils.dir_util import copy_tree
from pathlib import Path
from string import Template
from dataverk_cli.cli_utils import settings_loader
from .dataverk_base import DataverkBase
from .cli_utils import user_input
from dataverk.context.env_store import EnvStore
import yaml


class DataverkInit(DataverkBase):
    ''' Klasse for å opprette ny datapakke lokalt i et eksisterende repository for datapakker/datasett
    '''

    def __init__(self, settings: dict, envs: EnvStore):
        super().__init__(settings=settings, envs=envs)

        self._package_name = settings["package_name"]

    def run(self):
        ''' Entrypoint for dataverk init
        '''

        if self._folder_exists_in_repo(self._package_name):
            raise NameError(f'En mappe med navn {self._package_name} '
                            f'eksisterer allerede i repo {self.github_project}')

        if user_input.cli_question(f'Vil du opprette datapakken ({self._package_name}) i {self.github_project}? [j/n] '):
            try:
                self._create()
            except Exception:
                if os.path.exists(self._package_name):
                    rmtree(self._package_name)
                raise Exception(f'Klarte ikke generere datapakken {self._package_name}')
        else:
            print(f'Datapakken {self._package_name} ble ikke opprettet')

    def _create(self):
        ''' Oppretter ny datapakke med ønsket konfigurasjon
        '''

        self._create_datapackage_local()
        self._write_settings_file()
        self._edit_package_metadata()
        self._edit_jenkinsfile()
        self._edit_cronjob_config()

        print(f'Datapakken {self._package_name} er opprettet')

    def _create_datapackage_local(self):
        ''' Lager mappestrukturen for datapakken lokalt og henter template filer
        '''

        os.mkdir(self._package_name)

        templates_path = ""
        try:
            templates_loader = settings_loader.GitSettingsLoader(url=self.envs["TEMPLATES_REPO"])
            templates_path = templates_loader.download_to(".")
            copy_tree(os.path.join(str(templates_path), 'file_templates'), self._package_name)
        except OSError:
            raise OSError(f'Templates mappe eksisterer ikke.')
        finally:
            if os.path.exists(str(templates_path)):
                rmtree(str(templates_path))

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

        package_metadata['Datapakke_navn'] = self._package_name
        package_metadata['Bucket_navn'] = 'nav-opendata'

        try:
            with metadata_file_path.open('w') as metadatafile:
                json.dump(package_metadata, metadatafile, indent=2)
        except OSError:
            raise OSError(f'Finner ikke METADATA.json fil på Path({metadata_file_path})')

    def _edit_jenkinsfile(self):
        ''' Tilpasser Jenkinsfile til datapakken
        '''

        jenkinsfile_path = Path(self._package_name).joinpath("Jenkinsfile")
        tag_value = {"package_name": self._package_name,
                     "package_repo": self.github_project,
                     "package_path": self._package_name}

        try:
            with jenkinsfile_path.open('r') as jenkinsfile:
                jenkins_config = jenkinsfile.read()
        except OSError:
            raise OSError(f'Finner ikke Jenkinsfile på Path({jenkinsfile_path})')

        template = Template(jenkins_config)
        jenkins_config = template.safe_substitute(**tag_value)

        try:
            with jenkinsfile_path.open('w') as jenkinsfile:
                jenkinsfile.write(jenkins_config)
        except OSError:
            raise OSError(f'Finner ikke Jenkinsfile på Path{jenkinsfile_path})')

    def _edit_cronjob_config(self) -> None:
        """
        :return: None
        """

        cronjob_file_path = Path(self._package_name).joinpath("cronjob.yaml")

        try:
            with cronjob_file_path.open('r') as yamlfile:
                cronjob_config = yaml.load(yamlfile)
        except OSError:
            raise OSError(f'Finner ikke cronjob.yaml fil på Path({cronjob_file_path})')

        cronjob_config['metadata']['name'] = self._package_name
        cronjob_config['metadata']['namespace'] = self.settings["nais_namespace"]
        cronjob_config['spec']['jobTemplate']['spec']['template']['spec']['containers'][0]['name'] = self._package_name + '-cronjob'
        cronjob_config['spec']['jobTemplate']['spec']['template']['spec']['containers'][0]['image'] = self.settings["image_endpoint"] + self._package_name

        try:
            with cronjob_file_path.open('w') as yamlfile:
                yamlfile.write(yaml.dump(cronjob_config, default_flow_style=False))
        except OSError:
            raise OSError(f'Finner ikke cronjob.yaml fil på Path({cronjob_file_path})')