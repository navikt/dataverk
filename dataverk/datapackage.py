import pandas as pd
import os
import json
import errno
import datetime
from dataverk.connectors import OracleConnector
from dataverk.utils import notebook2script, publish_data


class Datapackage:

    def __init__(self, public=False):
        self.publish_publically = public # TODO: Foreløpig løsning. Hvordan skal dette håndteres?
        self.resources = {}
        self.datapackage_metadata = self._create_datapackage()

    def write_notebook(self):
        notebook2script()

    def add_resource(self, df: pd.DataFrame, dataset_name: str, dataset_description=''):
        self.resources[dataset_name] = df
        self.datapackage_metadata['Datasett'][dataset_name] = dataset_description

    def update_metadata(self, key: str, value: str):
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

    def _read_sql_return_pandas_frame(self, source, sql, connector='Oracle'):
        """
        Read pandas dataframe from SQL database
        """

        if (connector == 'Oracle'):
            conn = OracleConnector(source=source)

            if self._is_sql_file(source):
                return conn.get_pandas_df(source)

            path = self._get_path()
            with open(os.path.join(path, sql)) as f:
                query = f.read()

            return conn.get_pandas_df(query)

    def _read_sql_append_to_resources(self, source, sql, dataset_name, connector='Oracle'):
        """
        Read pandas dataframe from SQL database
        """

        if (connector == 'Oracle'):
            conn = OracleConnector(source=source)

            if self._is_sql_file(source):
                return conn.get_pandas_df(source)

            path = self._get_path()
            with open(os.path.join(path, sql)) as f:
                query = f.read()

            self.resources[dataset_name] = conn.get_pandas_df(query)

    def read_sql(self, source, sql, dataset_name=None, connector='Oracle'):
        if dataset_name is None:
            return self._read_sql_return_pandas_frame(source=source,
                                                      sql=sql,
                                                      connector=connector)
        else:
            self._read_sql_append_to_resources(source=source,
                                               sql=sql,
                                               connector=connector,
                                               dataset_name=dataset_name)

    def to_sql(self, df, table, schema, sink, connector='Oracle'):
        """Write records in dataframe to a SQL database table"""

        if (connector == 'Oracle'):
            conn = OracleConnector(source=sink)
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

    def _create_datapackage(self):
        today = datetime.date.today().strftime('%Y-%m-%d')
        dir_path = self._get_path()

        with open(os.path.join(dir_path, 'LICENSE.md'), encoding="utf-8") as f:
            licence = f.read()

        with open(os.path.join(dir_path, 'README.md'), encoding="utf-8") as f:
            readme = f.read()

        with open(os.path.join(dir_path, 'METADATA.json'), encoding="utf-8") as f:
            metadata = json.loads(f.read())

        with open(os.path.join(dir_path, 'METADATA.json'), 'w', encoding="utf-8") as f:
            metadata['Sist oppdatert'] = today
            metadata['Lisens'] = licence
            f.write(json.dumps(metadata, indent=2))

        return {
            'name': metadata.get('Id', ''),
            'title': metadata.get('Tittel', ''),
            'author': metadata.get('Opphav', ''),
            'status': metadata.get('Tilgangsrettigheter', ''),
            'Datasett': metadata.get('Datasett', {}),
            # TODO: unødvendig med lisens her siden lisensen ligger i metadata?
            'license': licence,
            'readme': readme,
            'metadata': json.dumps(metadata),
            'sources': metadata.get('Kilder', ''),
            'last_updated': today,
            'bucket_name': metadata.get('Bucket_navn', 'default-bucket-nav'),
            'datapackage_name': metadata.get('Datapakke_navn', 'default-pakke-nav')
        }

    def write_datapackage(self, datasets):
        dir_path = self._get_path()
        resources = []
        with open(dir_path + '/datapackage.json', 'w') as outfile:
            for filename, df in datasets.items():
                # TODO bruk Parquet i stedet for csv?
                resources.append(self._get_csv_schema(df, filename))

            self.datapackage_metadata['resources'] = resources

            json.dump(self.datapackage_metadata, outfile, indent=2, sort_keys=True)

            data_path = dir_path + '/data/'
            if not os.path.exists(data_path):
                try:
                    os.makedirs(data_path)
                except OSError as exc:  # Guard against race condition
                    if exc.errno != errno.EEXIST:
                        raise

            for filename, df in datasets.items():
                df.to_csv(dir_path + '/data/' + filename + '.csv', index=False, sep=';')

    def _datapackage_key_prefix(self, datapackage_name):
        return datapackage_name + '/'

    def publish(self):
        dir_path = self._get_path()
        self.write_datapackage(self.resources)

        publish_data.publish_s3_nais(dir_path=dir_path,
                                     bucket_name=self.datapackage_metadata["bucket_name"],
                                     datapackage_key_prefix=self._datapackage_key_prefix(self.datapackage_metadata["datapackage_name"]))
        if self.publish_publically:
            publish_data.publish_google_cloud(dir_path=dir_path,
                                              bucket_name=self.datapackage_metadata["bucket_name"],
                                              datapackage_key_prefix=self._datapackage_key_prefix(self.datapackage_metadata["datapackage_name"]))
