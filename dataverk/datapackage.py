import pandas as pd
import json
import datetime
from dataverk.connectors import OracleConnector, SQLiteConnector
from dataverk.utils import resource_discoverer, notebook2script, get_notebook_name
from dataverk.context import settings
from dataverk.utils.validators import validate_bucket_name, validate_datapackage_name
from pathlib import Path
from dataverk.context import EnvStore
from collections.abc import MutableMapping, MutableSequence


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
        """
        Adds a provided DataFrame as a resource in the Datapackage object with provided name and description.

        :param df: DataFrame to add as resource
        :param dataset_name: Name of the dataset
        :param dataset_description: Description of the dataset
        :return: None
        """
        self._verify_add_resource_input_types(df, dataset_name, dataset_description)
        self.resources[dataset_name] = df

    def add_view(self, name: str, resources: MutableSequence, title: str="", description: str="", attribution: str="", spec_type: str="simple",
                 spec: MutableMapping=None, type: str="", group: str="", series: MutableSequence=list(), row_limit: int=500):
        """
        Adds a view to the Datapackage object. A view is a specification of a visualisation the datapackage provides.

        :param name: View name
        :param resources: resource the view is for
        :param title: Title to be presented in the visualisation
        :param description: Description of the view
        :param attribution:
        :param spec_type: Spec type eg. (matplotlib, vega-lite)
        :param spec:
        :param type:
        :param group:
        :param series:
        :param row_limit:
        :return: None
        """
        if spec is None:
            spec = {"type": type,
                    "group": group,
                    "series": series}

        view = {'name': name,
                'title': title,
                'description': description,
                'attribution': attribution,
                'resources': resources,
                'specType': spec_type,
                'spec': spec,
                'transform': {
                    "limit": row_limit
                }}

        self.views.append(view)

    def _verify_update_metadata_input_types(self, key, value):
        if not isinstance(key, str):
            raise TypeError(f'Key must be of type string')
        if not isinstance(value, str):
            raise TypeError(f'Value must be of type string')

    def update_metadata(self, key: str, value: str):
        """
        Update the datapackage metadata.

        :param key: metadata field to update
        :param value: new value for given field
        :return: None
        """
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

        try:
            with self.dir_path.joinpath('LICENSE.md').open(mode='r', encoding="utf-8") as f:
                license = f.read()
        except OSError:
            license = "No LICENSE file available"

        try:   
            with self.dir_path.joinpath('README.md').open(mode='r', encoding="utf-8") as f:
                readme = f.read()
        except OSError:
            readme = "No README file available"

        try:
            with self.dir_path.joinpath('METADATA.json').open(mode='r', encoding="utf-8") as f:
                metadata = json.loads(f.read())
        except OSError:
            metadata = {}

        if metadata.get('public', False) is True:
            self.is_public = True
     
        metadata['updated'] = today
        metadata['version'] = "0.0.1"
        metadata['license'] = license
        metadata['bucket_name'] = metadata.get('bucket_name', 'default-bucket-nav-opendata')

        validate_bucket_name(metadata["bucket_name"])
        validate_datapackage_name(metadata["datapackage_name"])

        with self.dir_path.joinpath('METADATA.json').open(mode='w', encoding="utf-8") as f:
            f.write(json.dumps(metadata, indent=2))

        datapackage_metadata = {'readme': readme}
        datapackage_metadata.update(metadata)

        return datapackage_metadata

    def write_datapackage(self):
        """
        Writes the Datapackage object to output files, the datapackage files can then be published.

        :return: None
        """
        resources = []
        with self.dir_path.joinpath('datapackage.json').open('w') as outfile:
            for filename, df in self.resources.items():
                # TODO bruk Parquet i stedet for csv?
                resources.append(self._get_csv_schema(df, filename))

            self.datapackage_metadata['resources'] = resources

            self.datapackage_metadata['views'] = self.views

            json.dump(self.datapackage_metadata, outfile, indent=2, sort_keys=True)

            data_path = self.dir_path.joinpath('resources/')
            if not data_path.exists():
                data_path.mkdir()

            for filename, df in self.resources.items():
                df.to_csv(data_path.joinpath(filename + '.csv'), index=False, sep=',')

        try: #convert etl.ipynb notebook to etl.py when run in notebook
            shell = get_ipython().__class__.__name__
            if shell == 'ZMQInteractiveShell':
                notebook2script() 
        except:
            pass
