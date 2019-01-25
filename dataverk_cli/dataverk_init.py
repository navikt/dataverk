import json
from uuid import uuid4
from shutil import copy
from pathlib import Path
from string import Template
from importlib_resources import path
from dataverk_cli.cli.cli_utils import settings_loader
from .dataverk_base import DataverkBase, CONFIG_FILE_TYPES, BucketStorage
from collections.abc import Mapping


class DataverkInit(DataverkBase):
    ''' Klasse for å opprette ny datapakke lokalt i et eksisterende repository for datapakker/datasett
    '''

    def __init__(self, settings: Mapping, envs: Mapping):
        super().__init__(settings=settings, envs=envs)

        self._package_id = str(uuid4())

    def run(self):
        ''' Entrypoint for dataverk init
        '''

        try:
            self._create()
        except Exception:
            self._clean_up_files()
            raise Exception(f'Klarte ikke generere datapakken {self._settings_store["package_name"]}')

    def _create(self):
        ''' Oppretter ny datapakke med ønsket konfigurasjon
        '''

        self._create_datapackage_local()
        self._write_settings_file()
        self._edit_package_metadata()

        print(f'Datapakken {self._settings_store["package_name"]} er opprettet')

    def _create_datapackage_local(self):
        ''' Lager mappestrukturen for datapakken lokalt og henter template filer
        '''

        with path(package='dataverk_cli', resource='templates') as templates:
            for file in Path(templates).iterdir():
                if file.suffix in CONFIG_FILE_TYPES:
                    copy(str(Path(templates).joinpath(file)), '.')

        if self._settings_store.get("internal", "").lower() == "true":
            try:
                resource_url = self._envs["TEMPLATES_REPO"]
            except KeyError:
                raise KeyError(f"env_store({self._envs}) has to contain a TEMPLATES_REPO"
                               f" variable to initialize internal project ")
            else:
                settings_loader.load_template_files_from_resource(url=resource_url)
                self._edit_jenkinsfile()

    def _write_settings_file(self):

        settings_file_path = Path('settings.json')

        try:
            with settings_file_path.open('w') as settings_file:
                json.dump(self._settings_store, settings_file, indent=2)
        except OSError:
            raise OSError(f'Klarte ikke å skrive settings fil for datapakke')

    def _edit_package_metadata(self):
        '''  Tilpasser metadata fil til datapakken
        '''

        metadata_file_path = Path("METADATA.json")

        try:
            with metadata_file_path.open('r') as metadatafile:
                package_metadata = json.load(metadatafile)
        except OSError:
            raise OSError(f'Finner ikke METADATA.json fil på Path({metadata_file_path})')

        package_metadata['datapackage_name'] = self._settings_store["package_name"]
        package_metadata['title'] = self._settings_store["package_name"]
        package_metadata['id'] = self._package_id
        package_metadata['path'] = self._determine_bucket_path()

        try:
            with metadata_file_path.open('w') as metadatafile:
                json.dump(package_metadata, metadatafile, indent=2)
        except OSError:
            raise OSError(f'Finner ikke METADATA.json fil på Path({metadata_file_path})')

    def _determine_bucket_path(self):
        buckets = self._settings_store["bucket_storage_connections"]
        for bucket_type in self._settings_store["bucket_storage_connections"]:
            if self._is_publish_set(bucket_type=bucket_type):
                if BucketStorage(bucket_type) == BucketStorage.GITHUB:
                    return f'{buckets[bucket_type]["host"]}/{self._get_org_name()}/{self._settings_store["package_name"]}/master/'
                elif BucketStorage(bucket_type) == BucketStorage.DATAVERK_S3:
                    return f'{buckets[bucket_type]["host"]}/{buckets[bucket_type]["bucket"]}/{self._settings_store["package_name"]}'
                else:
                    raise NameError(f'Unsupported bucket type: {bucket_type}')

    def _is_publish_set(self, bucket_type: str):
        return self._settings_store["bucket_storage_connections"][bucket_type]["publish"].lower() == "true"

    def _edit_jenkinsfile(self):
        ''' Tilpasser Jenkinsfile til datapakken
        '''

        jenkinsfile_path = Path('Jenkinsfile')
        tag_value = {"package_name": self._settings_store["package_name"]}

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