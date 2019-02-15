import json
from uuid import uuid4
from shutil import copy
from pathlib import Path
from string import Template

from importlib_resources import path
from git import exc

from dataverk_cli.cli.cli_utils import settings_loader
from .dataverk_base import DataverkBase, CONFIG_FILE_TYPES
from collections.abc import Mapping
from dataverk_cli.cli.cli_utils import repo_info
from dataverk_cli.cli.cli_utils.user_message_templates import WARNING_TEMPLATE
from dataverk.connectors.bucket_connector_factory import BucketType


DEFAULT_SETTINGS_REPO = "https://github.com/navikt/dataverk-settings-public.git"


class DataverkInit(DataverkBase):
    """
        Class responsible for generating a new datapackage project with all required files and templates
    """

    def __init__(self, settings: Mapping, envs: Mapping):
        super().__init__(settings=settings, envs=envs)

        self._package_id = str(uuid4())

    def run(self) -> None:
        """ Entrypoint for dataverk init
        """

        try:
            self._create()
        except Exception:
            self._clean_up_files()
            raise Exception(f'Could not generate datapackage {self._settings_store["package_name"]}')

    def _create(self) -> None:
        """
        Calls required methods to create a new datapackage with the provided configurations

        :return: None
        """

        self._fetch_datapackage_project_files()
        if self._is_project_internal(self._settings_store):
            self._edit_jenkinsfile()

        self._write_settings_file()
        self._edit_package_metadata()

        print(f'Datapackage {self._settings_store["package_name"]} has been created!')

    def _fetch_datapackage_project_files(self):
        """
        Fetches required datapackage project files

        :return: None
        """
        resource_url = DEFAULT_SETTINGS_REPO
        if self._is_project_internal(self._settings_store):
            try:
                resource_url = self._envs["TEMPLATES_REPO"] # fetch internal file repo from env
            except KeyError:
                raise KeyError(f"env_store({self._envs}) has to contain a TEMPLATES_REPO"
                               f" variable to initialize internal project ")
        settings_loader.load_template_files_from_resource(url=resource_url)

    def _write_settings_file(self):

        settings_file_path = Path('settings.json')

        try:
            with settings_file_path.open('w') as settings_file:
                json.dump(self._settings_store, settings_file, indent=2)
        except OSError:
            raise OSError(f'Klarte ikke å skrive settings fil for datapakke')

    def _edit_package_metadata(self):
        """  Tilpasser metadata fil til datapakken
        """

        metadata_file_path = Path("METADATA.json")

        try:
            with metadata_file_path.open('r') as metadatafile:
                package_metadata = json.load(metadatafile)
        except OSError:
            raise OSError(f'Finner ikke METADATA.json fil på Path({metadata_file_path})')

        package_metadata['datapackage_name'] = self._settings_store["package_name"]
        package_metadata['title'] = self._settings_store["package_name"]
        package_metadata['id'] = self._package_id

        try:
            package_metadata['path'] = self._determine_bucket_path()
        except UserWarning as invalid_storage_path:
            print(WARNING_TEMPLATE.format(invalid_storage_path))
            package_metadata['path'] = ""

        try:
            with metadata_file_path.open('w') as metadatafile:
                json.dump(package_metadata, metadatafile, indent=2)
        except OSError:
            raise OSError(f'Finner ikke METADATA.json fil på Path({metadata_file_path})')

    def _determine_bucket_path(self):
        buckets = self._settings_store["bucket_storage_connections"]
        for bucket_type in self._settings_store["bucket_storage_connections"]:
            if self._is_publish_set(bucket_type=bucket_type):
                if BucketType(bucket_type) == BucketType.GITHUB:
                    try:
                        remote_url = repo_info.get_remote_url()
                    except exc.InvalidGitRepositoryError:
                        raise UserWarning(
                            f'Could not automatically determine storage path, you must manually specify storage path '
                            f'in METADATA.json. Github is chosen as storage location, but no remote repo is set for '
                            f'the datapackage repo.')
                    else:
                        return f'{buckets[bucket_type]["host"]}/{repo_info.get_org_name(remote_url)}/{self._settings_store["package_name"]}/master/'
                elif BucketType(bucket_type) == BucketType.DATAVERK_S3:
                    return f'{buckets[bucket_type]["host"]}/{buckets[bucket_type]["bucket"]}/{self._settings_store["package_name"]}'
                else:
                    raise NameError(f'Unsupported bucket type: {bucket_type}')

    def _is_publish_set(self, bucket_type: str):
        return self._settings_store["bucket_storage_connections"][bucket_type]["publish"].lower() == "true"

    def _edit_jenkinsfile(self):
        """ Tilpasser Jenkinsfile til datapakken
        """

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

    @staticmethod
    def _is_project_internal(settings_store):
        return  settings_store.get("internal", "").lower() == "true"
