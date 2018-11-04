
import pandas as pd
import os
import json
import errno
import datetime
import re
import uuid
from dataverk.connectors import OracleConnector, ElasticsearchConnector
from dataverk.utils import notebook2script, publish_data


class Datapackage:

    def __init__(self, public=False):
        if not isinstance(public, bool):
            raise TypeError("public parameter must be boolean")

        self.is_public = public
        self.resources = {}
        self.resources = {}
        self.dir_path = self._get_path()
        self.datapackage_metadata = self._create_datapackage()

    def write_notebook(self):
        # TODO: Hører dette hjemme her eller er det mer naturlig å beholde det på dv nivå? 
        notebook2script()

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

        # TODO raise error is not oracle and/or add more options

    def _read_sql_append_to_resources(self, source, sql, dataset_name, connector='Oracle', dataset_description=""):
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

            self.add_resource(df=conn.get_pandas_df(query),
                              dataset_name=dataset_name,
                              dataset_description=dataset_description)

    def read_sql(self, source, sql, dataset_name=None, connector='Oracle', dataset_description=""):
        if dataset_name is None:
            return self._read_sql_return_pandas_frame(source=source,
                                                      sql=sql,
                                                      connector=connector)
        else:
            self._read_sql_append_to_resources(source=source,
                                               sql=sql,
                                               connector=connector,
                                               dataset_name=dataset_name,
                                               dataset_description=dataset_description)

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

    def _verify_bucket_and_datapackage_names(self, metadata):
        # TODO Erik: Krever S3 at det kun benyttes bokstaver?
        # valid_name_pattern = '(^[a-z])([a-z\-])+([a-z])$'
        valid_name_pattern = '(^[a-z0-9])([a-z0-9\-])+([a-z0-9])$'
        if not re.match(pattern=valid_name_pattern, string=metadata["Bucket_navn"]):
            raise NameError("Invalid bucket name: Must be lowercase letters or numbers, words separated by '-', and cannot "
                            "start or end with '-'")
        if not re.match(pattern=valid_name_pattern, string=metadata["Datapakke_navn"]):
            raise NameError("Invalid datapackage name: Must be lowercase letters or numbers, words separated by '-', and cannot "
                            "start or end with '-'")

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

        if metadata.get('Offentlig', False) == True:
            self.is_public = True
        
        if metadata.get('Public', False) == True:
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

    def write_datapackage(self, datasets):
        resources = []
        with open(self.dir_path + '/datapackage.json', 'w') as outfile:
            for filename, df in datasets.items():
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

            for filename, df in datasets.items():
                df.to_csv(self.dir_path + '/data/' + filename + '.csv', index=False, sep=';')

    def _datapackage_key_prefix(self, datapackage_name):
        return datapackage_name + '/'

    def publish(self, destination=['nais', 'gcs']):
        self.write_datapackage(self.resources)
        # TODO: add views

        if 'nais' in destination:
            publish_data.publish_s3_nais(dir_path=self.dir_path,
                bucket_name=self.datapackage_metadata["bucket_name"],
                datapackage_key_prefix=self._datapackage_key_prefix(self.datapackage_metadata["datapackage_name"]))
    
        if self.is_public and 'gcs' in destination:
            publish_data.publish_google_cloud(dir_path=self.dir_path,
                bucket_name=self.datapackage_metadata["bucket_name"],
                datapackage_key_prefix=self._datapackage_key_prefix(self.datapackage_metadata["datapackage_name"]))

            try: 
                es = ElasticsearchConnector('public')
                id = self.datapackage_metadata['datapackage_name']
                js = json.dumps(self.datapackage_metadata)
                es.write(id, js)
            except:
                pass 

