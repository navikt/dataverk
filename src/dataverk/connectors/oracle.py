import time
import cx_Oracle
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from urllib import parse
from collections.abc import Mapping
from dataverk.connectors.abc.db_base import DBBaseConnector


class OracleConnector(DBBaseConnector):
    """Common oracle connector methods

    Troubleshooting:

    Note: Oracle instant client libraries required to be installed in order to use cx_Oracle

    Mac:

    ´´´
    unzip instant client zip file from oracle to ~/opt/oracle/instantclient_12_2
    ln -s ~/opt/oracle/instantclient_12_2/libclntsh.dylib.12.1 /usr/local/lib/
    ´´´

    """

    def __init__(self, settings_store: Mapping, source=None):
        super().__init__()

        self._settings = settings_store
        self._source = source
        self._df = None
        self._dsn = None
    
        if source not in settings_store["db_connection_strings"]:
            raise ValueError(f'Database connection string not found in settings file. '
                             f'Unable to establish connection to database: {source}')

        self._db = self._parse_connection_string(settings_store["db_connection_strings"][source])

        if 'service_name' in self._db:
            self.dsn = cx_Oracle.makedsn(host=self._db['host'], port=self._db['port'], service_name=self._db['service_name'])
        elif 'sid' in self._db:
            self.dsn = cx_Oracle.makedsn(host=self._db['host'], port=self._db['port'], sid=self._db['sid'])
        else:
            raise ValueError(f'Invalid connection description. Neither "service name" nor "sid" specified for {self._source}')

    def _parse_connection_string(self, connection_string):
        res = parse.urlparse(connection_string)

        return {
                'user': res.username,
                'password': res.password,
                'host': res.hostname,
                'port': res.port,
                'service_name': res.path[1:]
               }

    def get_pandas_df(self, query, arraysize=100000):
        start_time = time.time()

        if self._df:
            self.log(f'{len(self._df)} records returned from cached dataframe. Query: {query}')
            return self._df

        self.log(f'Establishing connection to Oracle database: {self._source}')

        try: 
            conn = cx_Oracle.connect(
                user=self._db['user'],
                password=self._db['password'],
                dsn=self.dsn,
                encoding='utf-8'
            ) 

            cur = conn.cursor()
            cur.arraysize = arraysize
            cur.execute(query)
            col_names = [x[0] for x in cur.description]
            results = cur.fetchall()
            cur.close()
            conn.close()

            end_time = time.time()

            df = pd.DataFrame(results, columns=col_names)
            
            self.log(f'{len(df)} records returned in {end_time - start_time} seconds. Query: {query}')

            self._df = df

            return df

        except cx_Oracle.DatabaseError as dberror:
            self.log(dberror)

    def persist_pandas_df(self, table, schema=None, df=None, chunksize=10000, if_exists='replace'):

        if 'service_name' in self._db:
            engine = create_engine(f"oracle+cx_oracle://{self._db['user']}:{self._db['password']}@"
                                   f"{self._db['host']}:{self._db['port']}/?service_name={self._db['service_name']}")
        elif 'sid' in self._db:
            engine = create_engine(f"oracle+cx_oracle://{self._db['user']}:{self._db['password']}@"
                                   f"{self._db['host']}:{self._db['port']}/{self._db['sid']}")
        else:
            raise ValueError(f'Neither "service_name" nor "sid" found in database connection string')

        if schema is None:
            schema = "dataverk"

            self.log(f'Persisting {len(df)} records to table: {table}'
                     f' in schema: {schema} in Oracle database: {self._source}')

        try:
            # using sqlalchemy pandas support
            df.to_sql(table, engine, schema=schema, if_exists=if_exists, chunksize=chunksize)
            return len(df)
        except SQLAlchemyError as e:
            error = str(e.__dict__['orig'])
            return error
