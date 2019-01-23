import subprocess
import json
from dataverk_cli.dataverk_base import DataverkBase, BucketStorage
from dataverk_cli.scheduling import scheduler_factory
from dataverk.context.env_store import EnvStore
from dataverk.context.settings import SettingsStore
from pathlib import Path


class DataverkSchedule(DataverkBase):
    def __init__(self, settings: SettingsStore, envs: EnvStore):
        super().__init__(settings=settings, envs=envs)

        self._scheduler = scheduler_factory.create_scheduler(settings_store=settings, env_store=envs)

    def run(self):
        if not self._datapackage_exists_in_remote_repo():
            raise FileNotFoundError(f'Datapakken må eksistere i remote repositoriet før man kan eksekvere '
                                   f'<dataverk-cli schedule>. git add->commit->push av datapakken og prøv på nytt.')

        try:
            self._schedule_job()
        except Exception:
            raise Exception(f'Klarte ikke sette opp pipeline for datapakke {self._settings_store["package_name"]}')

        print(f'Jobb for datapakke {self._settings_store["package_name"]} er satt opp/rekonfigurert. '
              f'For å fullføre oppsett av pipeline må endringer pushes til remote repository')

    def _get_org_name(self):
        url_list = Path(self._github_project).parts

        return url_list[2]

    def _datapackage_exists_in_remote_repo(self):
        try:
            subprocess.check_output(["git", "cat-file", "-e", f'origin/master:Jenkinsfile'])
            return True
        except subprocess.CalledProcessError:
            return False

    def _schedule_job(self):
        ''' Setter opp schedulering av job for datapakken
        '''
        self._edit_package_metadata()
        self._scheduler.configure_job()

    def _edit_package_metadata(self):
        '''  Tilpasser metadata fil til datapakken
        '''

        metadata_file_path = Path('METADATA.json')

        try:
            with metadata_file_path.open('r') as metadatafile:
                package_metadata = json.load(metadatafile)
        except OSError:
            raise OSError(f'Finner ikke METADATA.json fil på Path({metadata_file_path})')

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