import json
import urllib3
from dataverk.connectors.elasticsearch import ElasticsearchConnector
from dataverk.connectors.bucket_connector_factory import get_storage_connector
from dataverk.utils.resource_discoverer import search_for_files
from dataverk.utils.env_store import EnvStore
from dataverk.utils import settings
from dataverk.utils import publish_data
from pathlib import Path


class PublishDataPackage:

    def __init__(self):
        self.resource_files = search_for_files(start_path=Path('.'), file_names=('.env', 'settings.json'), levels=4)

        try:
            self.env_store = EnvStore(Path(self.resource_files[".env"]))
        except KeyError:
            self.env_store = None

        self.package_settings = settings.create_settings_store(settings_file_path=Path(self.resource_files["settings.json"]),
                                                               env_store=self.env_store)
        self.package_metadata = self._read_metadata()

    def _read_metadata(self):
        try:
            with self._package_top_dir().joinpath('datapackage.json').open(mode='r') as metadata_file:
                return json.load(metadata_file)
        except OSError:
            raise OSError(f'No datapackage.json file found in datapackage')

    def _get_elastic_index(self, bucket_type: str) -> ElasticsearchConnector:
        if bucket_type == "google_cloud":
            return ElasticsearchConnector(settings=self.package_settings, host="elastic_public")
        elif bucket_type == "AWS_S3":
            return ElasticsearchConnector(settings=self.package_settings, host="elastic_private")

    def _package_top_dir(self) -> Path:
        return Path(".").parent

    def _datapackage_key_prefix(self, datapackage_name: str):
        return datapackage_name + '/'

    def publish(self):
        print(f'Publishing package {self.package_settings["package_name"]}')

        for bucket_type in self.package_settings["bucket_storage_connections"]:
            if self.package_settings["bucket_storage_connections"][bucket_type]["publish"] == "True":
                publish_data.upload_to_storage_bucket(dir_path=str(self._package_top_dir()),
                                                      conn=get_storage_connector(bucket_type=bucket_type,
                                                                                 bucket_name=self.package_metadata.get("bucket_name"),
                                                                                 settings=self.package_settings,
                                                                                 encrypted=False),
                                                      datapackage_key_prefix=self._datapackage_key_prefix(
                                                          self.package_settings["package_name"]))

                try:
                    es = self._get_elastic_index(bucket_type=bucket_type)
                    id = self.package_settings["package_name"]
                    js = json.dumps(self.package_metadata)
                    es.write(id, js)
                except urllib3.exceptions.LocationValueError:
                    print(f'write to elastic search failed, host_uri could not be resolved') # TODO: Må egentlig kaste exception her, men vi har ingen public ES index ennå
                except Exception:
                    print(f'Exception: write to elastic index {es.host_uri} failed') # TODO: Må egentlig kaste exception her, men vi har ingen public ES index ennå

        print(f'Package {self.package_settings["package_name"]} successfully published')


def publish_datapackage():
    datapackage = PublishDataPackage()
    datapackage.publish()
