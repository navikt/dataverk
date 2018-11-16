import pandas as pd
import os
import json
import errno
import datetime
import re
import uuid
from dataverk.connectors import OracleConnector, ElasticsearchConnector
from dataverk.utils import resource_discoverer, publish_data
from dataverk.utils.settings_store import SettingsStore
from pathlib import Path
from dataverk.utils import EnvStore


class Datapackage:

    def __init__(self, public=False, resource_files: dict=None, search_start_path: str="."):
        if not isinstance(public, bool):
            raise TypeError("public parameter must be boolean")

        self.is_public = public
        self.resources = {}
        self.dir_path = self._get_path()
        self.datapackage_metadata = self._create_datapackage()

        if resource_files is not None:
            self.resource_files = resource_files
        else:
            self.resource_files = resource_discoverer.search_for_files(start_path=Path(search_start_path),
                                                                  file_names=('settings.json', '.env'), levels=2)

        try:
            env_store = EnvStore(Path(self.resource_files[".env"]))
        except KeyError:
            env_store = None

        self.settings = SettingsStore(settings_json_url=Path(self.resource_files["settings.json"]), env_store=env_store)

    def _verify_add_resource_input_types(self, df, dataset_name, dataset_description):
        if not isinstance(df, pd.DataFrame):
            raise TypeError("df must be of type pandas.Dataframe()")
        if not isinstance(dataset_name, str):
            raise TypeError("dataset_name must be of type string")
        if not isinstance(dataset_description, str):
            raise TypeError("dataset_description must be of type string")

    def add_resource(self, df: pd.DataFrame, dataset_name: str, dataset_description: str=""):
        self._verify_add_resource_input_types(df, dataset_name, dataset_description)
        self.resources[dataset_name] = df
        self.datapackage_metadata['Datasett'][dataset_name] = dataset_description

    def _verify_update_metadata_input_types(self, key, value):
        if not isinstance(key, str):
            raise TypeError("Key must be of type string")
        if not isinstance(value, str):
            raise TypeError("Value must be of type string")

    def update_metadata(self, key: str, value: str):
        self._verify_update_metadata_input_types(key, value)
        self.datapackage_metadata[key] = value

    def add_view(self):
        pass

    def _get_path(self):
        if 'current_path' not in globals():
            path = os.getcwd()
        return os.path.abspath(os.path.join(path, os.pardir))

    def _is_sql_file(self, source):
        if '.sql' in source:
            return True
        return False

    def read_sql(self, source, sql, dataset_name=None, connector='Oracle', dataset_description=""):
        """
        Read pandas dataframe from SQL database
        """

        if connector == 'Oracle':
            conn = OracleConnector(source=source, settings=self.settings)

            if self._is_sql_file(source):
                df = conn.get_pandas_df(source)
            else:
                path = self._get_path()
                with open(os.path.join(path, sql)) as f:
                    query = f.read()
                df = conn.get_pandas_df(query)
        else:
            raise TypeError("Connector type '" + connector + "' is not supported")

        # TODO add more connector options

        if dataset_name is None:
            return df
        else:
            self.add_resource(df=df,
                              dataset_name=dataset_name,
                              dataset_description=dataset_description)

    def to_sql(self, df, table, schema, sink, connector='Oracle'):
        """Write records in dataframe to a SQL database table"""

        if (connector == 'Oracle'):
            conn = OracleConnector(source=sink, settings=self.settings)
            return conn.persist_pandas_df(table, schema, df)

    def _get_csv_schema(self, df, filename):
        fields = []

        for name, dtype in zip(df.columns, df.dtypes):
            # TODO : Bool and others? Move to utility method
            if str(dtype) == 'object':
                dtype = 'string'
            else:
                dtype = 'number'

            fields.append({'name': name, 'description': '', 'type': dtype})

        return {
            'name': filename,
            'path': 'data/' + filename + '.csv',
            'format': 'csv',
            'mediatype': 'text/csv',
            'schema': {'fields': fields}
        }

    def _verify_bucket_and_datapackage_names(self, metadata):
        valid_name_pattern = '(^[a-z0-9])([a-z0-9\-])+([a-z0-9])$'
        if not re.match(pattern=valid_name_pattern, string=metadata["Bucket_navn"]):
            raise NameError("Invalid bucket name (" + metadata["Bucket_navn"] + "): "
                            "Must be lowercase letters or numbers, words separated by '-', and cannot start or end with '-'")
        if not re.match(pattern=valid_name_pattern, string=metadata["Datapakke_navn"]):
            raise NameError("Invalid datapackage name (" + metadata["Datapakke_navn"] +"): "
                            "Must be lowercase letters or numbers, words separated by '-', and cannot start or end with '-'")

    def _create_datapackage(self):
        today = datetime.date.today().strftime('%Y-%m-%d')
        guid = uuid.uuid4().hex

        try:
            with open(os.path.join(self.dir_path, 'LICENSE.md'), encoding="utf-8") as f:
                license = f.read()
        except:
            license="No LICENSE file available"
            pass

        try:   
            with open(os.path.join(self.dir_path, 'README.md'), encoding="utf-8") as f:
                readme = f.read()
        except:
            readme="No README file available"
            pass

        metadata = {}

        try:
            with open(os.path.join(self.dir_path, 'METADATA.json'), encoding="utf-8") as f:
                metadata = json.loads(f.read())
        except:
            pass

        if metadata.get('Offentlig', False) is True:
            self.is_public = True
        
        if metadata.get('Public', False) is True:
            self.is_public = True
     
        metadata['Sist oppdatert'] = today
        metadata['Lisens'] = license
        metadata['Bucket_navn'] = metadata.get('Bucket_navn', 'default-bucket-nav')
        metadata['Datapakke_navn'] = metadata.get('Datapakke_navn', guid)

        with open(os.path.join(self.dir_path, 'METADATA.json'), 'w', encoding="utf-8") as f:
            f.write(json.dumps(metadata, indent=2))

        self._verify_bucket_and_datapackage_names(metadata)

        return {
            'name': metadata.get('Id', ''),
            'title': metadata.get('Tittel', ''),
            'author': metadata.get('Opphav', ''),
            'status': metadata.get('Tilgangsrettigheter', ''),
            'Datasett': metadata.get('Datasett', {}),
            'license': license,
            'readme': readme,
            'metadata': metadata,
            'sources': metadata.get('Kilder', ''),
            'last_updated': today,
            'bucket_name': metadata['Bucket_navn'],
            'datapackage_name': metadata['Datapakke_navn']
        }

    def write_datapackage(self):
        resources = []
        with open(self.dir_path + '/datapackage.json', 'w') as outfile:
            for filename, df in self.resources.items():
                # TODO bruk Parquet i stedet for csv?
                resources.append(self._get_csv_schema(df, filename))

            self.datapackage_metadata['resources'] = resources

            json.dump(self.datapackage_metadata, outfile, indent=2, sort_keys=True)

            data_path = self.dir_path + '/data/'
            if not os.path.exists(data_path):
                try:
                    os.makedirs(data_path)
                except OSError as exc:  # Guard against race condition
                    if exc.errno != errno.EEXIST:
                        raise

            for filename, df in self.resources.items():
                df.to_csv(data_path + filename + '.csv', index=False, sep=';')

    def _datapackage_key_prefix(self, datapackage_name):
        return datapackage_name + '/'

    def publish(self, destination=['nais', 'gcs']):
        self.write_datapackage()
        # TODO: add views

        if 'nais' in destination:
            publish_data.publish_s3_nais(dir_path=self.dir_path,
                                         datapackage_key_prefix=self._datapackage_key_prefix(self.datapackage_metadata["datapackage_name"]),
                                         settings=self.settings)
    
        if self.is_public and 'gcs' in destination:
            publish_data.publish_google_cloud(dir_path=self.dir_path,
                                              datapackage_key_prefix=self._datapackage_key_prefix(self.datapackage_metadata["datapackage_name"]),
                                              settings=self.settings)

            try: 
                es = ElasticsearchConnector('public')
                id = self.datapackage_metadata['datapackage_name']
                js = json.dumps(self.datapackage_metadata)
                es.write(id, js)
            except:
                pass 

