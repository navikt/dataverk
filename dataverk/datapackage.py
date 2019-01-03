import pandas as pd
import json
import datetime
import uuid
from dataverk.connectors import OracleConnector
from dataverk.utils import resource_discoverer
from dataverk.context import settings
from dataverk.utils.validators import validate_bucket_name, validate_datapackage_name
from pathlib import Path
from dataverk.context import EnvStore


class Datapackage:

    def __init__(self, resource_files: dict=None, search_start_path: str="."):
        self.is_public = False
        self.resources = {}
        self.views = []
        self.dir_path = self._package_top_dir()
        self.datapackage_metadata = self._create_datapackage()

        if resource_files is not None:
            self.resource_files = resource_files
        else:
            self.resource_files = resource_discoverer.search_for_files(start_path=Path(search_start_path),
                                                                       file_names=('settings.json', '.env'), levels=4)

        try:
            env_store = EnvStore(Path(self.resource_files[".env"]))
        except KeyError:
            env_store = None

        try: 
            self.settings = settings.singleton_settings_store_factory(settings_file_path=Path(self.resource_files["settings.json"]),
                                                       env_store=env_store)
        except:
            self.settings = None
            pass

    def _verify_add_resource_input_types(self, df, dataset_name, dataset_description):
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f'df must be of type pandas.Dataframe()')
        if not isinstance(dataset_name, str):
            raise TypeError(f'dataset_name must be of type string')
        if not isinstance(dataset_description, str):
            raise TypeError(f'dataset_description must be of type string')

    def add_resource(self, df: pd.DataFrame, dataset_name: str, dataset_description: str=""):
        self._verify_add_resource_input_types(df, dataset_name, dataset_description)
        self.resources[dataset_name] = df
        self.datapackage_metadata['Datasett'][dataset_name] = dataset_description

    def add_view(self, name: str, resource: str, columns: None, view_type: str="Simple", title: str="", description: str=""):
        if columns is None:
            columns = []
        view = {
            'name': name,
            'type': view_type,
            'resource': resource,
            'columns': columns,
            'title': title
        }
        self.views.append(view)

    def _verify_update_metadata_input_types(self, key, value):
        if not isinstance(key, str):
            raise TypeError(f'Key must be of type string')
        if not isinstance(value, str):
            raise TypeError(f'Value must be of type string')

    def update_metadata(self, key: str, value: str):
        self._verify_update_metadata_input_types(key, value)
        self.datapackage_metadata[key] = value

    def _package_top_dir(self) -> Path:
        return Path(".").parent.absolute()

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
                path = self._package_top_dir()
                with path.joinpath(sql).open(mode='r') as f:
                    query = f.read()
                df = conn.get_pandas_df(query)

        elif connector == 'SQLite':
            conn = SQLiteConnector()
            if self._is_sql_file(source):
                df = conn.get_pandas_df(source)
            else:
                path = self._package_top_dir()
                with path.joinpath(sql).open(mode='r') as f:
                    query = f.read()
                df = conn.get_pandas_df(query)

        else:
            raise TypeError(f'Connector type {connector} is not supported')

        # TODO add more connector options

        if dataset_name is None:
            return df
        else:
            self.add_resource(df=df,
                              dataset_name=dataset_name,
                              dataset_description=dataset_description)

    def to_sql(self, df, table, schema, sink, connector='Oracle'):
        """Write records in dataframe to a SQL database table"""

        if connector == 'Oracle':
            conn = OracleConnector(source=sink, settings=self.settings)
            return conn.persist_pandas_df(table, schema, df)

        elif connector == 'SQLlite':
            conn = SQLiteConnector()
            return conn.persist_pandas_df(table, df)

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
            'path': 'resources/' + filename + '.csv',
            'format': 'csv',
            'mediatype': 'text/csv',
            'schema': {'fields': fields}
        }

    def _create_datapackage(self):
        today = datetime.date.today().strftime('%Y-%m-%d')
        guid = uuid.uuid4().hex

        try:
            with self.dir_path.joinpath('LICENSE.md').open(mode='r', encoding="utf-8") as f:
                license = f.read()
        except OSError:
            license = "No LICENSE file available"
            pass

        try:   
            with self.dir_path.joinpath('README.md').open(mode='r', encoding="utf-8") as f:
                readme = f.read()
        except OSError:
            readme = "No README file available"
            pass

        metadata = {}

        try:
            with self.dir_path.joinpath('METADATA.json').open(mode='r', encoding="utf-8") as f:
                metadata = json.loads(f.read())
        except OSError:
            pass

        if metadata.get('Offentlig', False) is True:
            self.is_public = True

        if metadata.get('Public', False) is True:
            self.is_public = True
     
        metadata['Sist oppdatert'] = today
        metadata['Lisens'] = license
        metadata['Bucket_navn'] = metadata.get('Bucket_navn', 'default-bucket-nav-opendata')
        metadata['Datapakke_navn'] = metadata.get('Datapakke_navn', guid)

        validate_bucket_name(metadata["Bucket_navn"])
        validate_datapackage_name(metadata["Datapakke_navn"])

        with self.dir_path.joinpath('METADATA.json').open(mode='w', encoding="utf-8") as f:
            f.write(json.dumps(metadata, indent=2))

        return {
            'name': metadata.get('Id', ''),
            'title': metadata.get('Tittel', ''),
            'author': metadata.get('Opphav', ''),
            'status': metadata.get('Tilgangsrettigheter', ''),
            'Datasett': metadata.get('Datasett', {}),
            'readme': readme,
            'license': license,
            'metadata': metadata,
            'sources': metadata.get('Kilder', ''),
            'last_updated': today,
            'bucket_name': metadata['Bucket_navn'],
            'datapackage_name': metadata['Datapakke_navn']
        }

    def write_datapackage(self):
        resources = []
        with self.dir_path.joinpath('datapackage.json').open('w') as outfile:
            for filename, df in self.resources.items():
                # TODO bruk Parquet i stedet for csv?
                resources.append(self._get_csv_schema(df, filename))

            self.datapackage_metadata['resources'] = resources

            self.datapackage_metadata['views'] = json.dumps(self.views)

            json.dump(self.datapackage_metadata, outfile, indent=2, sort_keys=True)

            data_path = self.dir_path.joinpath('resources/')
            if not data_path.exists():
                data_path.mkdir()

            for filename, df in self.resources.items():
                df.to_csv(data_path.joinpath(filename + '.csv'), index=False, sep=';')
