import time
import cx_Oracle
import pandas as pd
import dask
import dask.dataframe as dd
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
    
        if self._source not in settings_store["db_connection_strings"]:
            raise ValueError(f'Database connection string not found in settings file. '
                             f'Unable to establish connection to database: {self._source}')

    def get_dask_df(self, query, where_values) -> dd.DataFrame:
        parsed_conn_string = self._parse_connection_string(self._settings["db_connection_strings"][self._source])
        return self._read_sql_query_dask(parsed_conn_string, query, where_values)

    def get_pandas_df(self, query, arraysize=100000):
        start_time = time.time()

        self.log(f'Establishing connection to Oracle database: {self._source}')

        parsed_conn_string = self._parse_connection_string(self._settings["db_connection_strings"][self._source])
        dsn = cx_Oracle.makedsn(host=parsed_conn_string['host'], port=parsed_conn_string['port'], service_name=parsed_conn_string['service_name'])

        try: 
            conn = self._get_db_conn(parsed_conn_string, dsn)

            cur = conn.cursor()
            cur.arraysize = arraysize
            cur.execute(query)
            col_names = [x[0] for x in cur.description]

            results = self._fetch_all(cur)

            end_time = time.time()

            df = pd.DataFrame(results, columns=col_names)

            cur.close()
            conn.close()

            self.log(f'{len(df)} records returned in {end_time - start_time} seconds. Query: {query}')

            return df

        except cx_Oracle.DatabaseError as dberror:
            self.log(dberror)

    def persist_pandas_df(self, table, schema=None, df=None, chunksize=10000, if_exists='replace'):
        parsed_conn_string = self._parse_connection_string(self._settings["db_connection_strings"][self._source])

        if 'service_name' in parsed_conn_string:
            engine = create_engine(f"oracle+cx_oracle://{parsed_conn_string['user']}:{parsed_conn_string['password']}@"
                                   f"{parsed_conn_string['host']}:{parsed_conn_string['port']}/"
                                   f"?service_name={parsed_conn_string['service_name']}")
        else:
            raise ValueError(f'"service_name" not found database connection string')

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

    def _read_sql_query_dask(self, parsed_conn_string, sql, where_values):
        dload = dask.delayed(self._load_df_part)
        parts = [dload(sql, parsed_conn_string, where) for where in where_values]
        return dd.from_delayed(parts)

    def _load_df_part(self, sql, parsed_conn_string, where):
        dsn = cx_Oracle.makedsn(host=parsed_conn_string['host'], port=parsed_conn_string['port'],
                                service_name=parsed_conn_string['service_name'])
        db_conn = self._get_db_conn(parsed_conn_string, dsn)
        partial_sql = f"{sql} {where}"
        return pd.read_sql(partial_sql, db_conn)

    @staticmethod
    def _parse_connection_string(connection_string):
        res = parse.urlparse(connection_string)

        return {
                'user': res.username,
                'password': res.password,
                'host': res.hostname,
                'port': res.port,
                'service_name': res.path[1:]
               }

    @staticmethod
    def _get_db_conn(parsed_conn_string, dsn):
        return cx_Oracle.connect(
                user=parsed_conn_string['user'],
                password=parsed_conn_string['password'],
                dsn=dsn,
                encoding='utf-8'
            )

    def _fetch_all(self, cursor):
        return [self._read_table_row(row) for row in cursor.fetchall()]

    def _read_table_row(self, row):
        new_row = []
        for elem in row:
            if self._is_oracle_LOB(elem):
                new_row.append(elem.read())
            else:
                new_row.append(elem)
        return new_row

    def _is_oracle_LOB(self, elem):
        return type(elem) == cx_Oracle.LOB
