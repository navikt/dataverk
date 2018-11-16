import time
import cx_Oracle
import pandas as pd
from sqlalchemy import create_engine

from urllib import parse
from dataverk.connectors import BaseConnector
from dataverk.utils.settings_store import SettingsStore

# Oracle
class OracleConnector(BaseConnector):
    """Common oracle connector methods

    Troubleshooting:

    Note: Oracle instant client libraries required to be installed in order to use cx_Oracle

    Mac:

    ´´´
    unzip instant client zip file from oracle to ~/opt/oracle/instantclient_12_2
    ln -s ~/opt/oracle/instantclient_12_2/libclntsh.dylib.12.1 /usr/local/lib/
    ´´´

    """

    def __init__(self, settings: SettingsStore, source=None):
        super(OracleConnector, self).__init__()

        self.settings = settings
        self.source = source
        self.df = None
        self.dsn = None
    
        if source not in settings["db_connection_strings"]:
            raise ValueError(f'Database connection string not found in settings file.\
             Unable to establish connection to database: {source}')

        db = self._parse_connection_string(settings["db_connection_strings"][source])
        self.db = db

        if 'service_name' in db:
            self.dsn = cx_Oracle.makedsn(
                host = db['host'],
                port = db['port'],
                service_name = db['service_name']
            )

        if 'sid' in db:
            self.dsn = cx_Oracle.makedsn(
                host = db['host'],
                port = db['port'],
                sid = db['sid']
            )

        assert self.dsn is not None, f'Invalid connection description. Neither "service name" nor "sid" specified for {self.source}'

    def _parse_connection_string(self, connection_string):
        res = parse.urlparse(connection_string)

        return {
                    'user': res.username,
                    'password': res.password,
                    'host': res.hostname,
                    'port': res.port,
                    'service_name': res.path[1:]
               }


    def get_pandas_df(self, sql, arraysize=100000):

        start_time = time.time()

        if self.df:
            self.log(f'{len(self.df)} records returned from cached dataframe. Query: {sql}')
            return self.df

        self.log(f'Establishing connection to Oracle database: {self.source}')

        try: 
            conn = cx_Oracle.connect(
                user=self.db['user'], 
                password=self.db['password'],
                dsn = self.dsn,
                encoding = 'utf-8'
            ) 

            cur = conn.cursor()
            cur.arraysize = arraysize
            cur.execute(sql)
            col_names = [x[0] for x in cur.description]
            results = cur.fetchall()
            cur.close()
            conn.close()

            end_time = time.time()

            df = pd.DataFrame(results, columns=col_names)
            
            self.log(f'{len(df)} records returned in {end_time - start_time} seconds. Query: {sql}')

            self.df = df

            return df

        except cx_Oracle.DatabaseError as dberror:
            self.log(dberror)

    def persist_pandas_df(self, table, schema=None, df=None, chunksize=10000):

        if 'service_name' in self.db:
            engine = create_engine(f"oracle+cx_oracle://{self.db['user']}:{self.db['password']}@{self.db['host']}:{self.db['port']}/?service_name={self.db['service_name']}")
        
        if 'sid' in self.db: 
            engine = create_engine(f"oracle+cx_oracle://{self.db['user']}:{self.db['password']}@{self.db['host']}:{self.db['port']}/{self.db['sid']}")
        
        #exists = engine.dialect.has_table(engine, table, schema = schema)

        if schema is None:
            schema = self.get_user()
       
        try:
            engine.execute(f'DROP TABLE {schema}.{table}')
        except:
            pass

        # using sqlalchemy pandas support
        self.log(f'Persisting {len(df)} records to table: {table} in schema: {schema} in Oracle database: {self.source}')
        df.to_sql(table, engine, schema=schema, if_exists='replace', chunksize=chunksize)

        return len(df)
