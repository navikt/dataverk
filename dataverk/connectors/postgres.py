import time
import pandas as pd
#from postgresadapter import PostgresAdapter
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dataverk.connectors import SQLDbConnector
from collections.abc import Mapping

class PostgresConnector(SQLDbConnector):


    def __init__(self, settings_store: Mapping, source=None):
        super(PostgresConnector, self).__init__()

        self.settings = settings_store
        self.source = source
        self.df = None

        if source not in settings_store["db_connection_strings"]:
            raise ValueError(f'Database connection string not found in settings file.\
             Unable to establish connection to PostgreSQL database: {source}')

        self.db = settings_store["db_connection_strings"][source]

        self.engine = create_engine(self.db)


    def get_pandas_df(self, sql, arraysize=100000):

        start_time = time.time()

        if self.df:
            self.log(f'{len(self.df)} records returned from cached dataframe. Query: {sql}')
            return self.df

        self.log(f'Establishing connection to PostgreSQL database: {self.source}')

        try: 
            #adapter = PostgresAdapter(self.db, query = sql, self.pg_kwargs)
            df = pd.read_sql_query(sql, self.engine)
            end_time = time.time()
        
            self.log(f'{len(df)} records returned in {end_time - start_time} seconds. Query: {sql}')

            self.df = df

            return df

        except SQLAlchemyError as e:
            error = str(e.__dict__['orig'])
            return error

    def persist_pandas_df(self, table, schema=None, df=None, chunksize=10000, if_exists='replace'):

        engine = create_engine(self.db)

        _table = table

        if schema is not None:
           _table = f'{schema}.{table}'
       
        try:
            self.log(f'Persisting {len(df)} records to table: {_table} in PostgreSQL database: {self.source}')
            df.to_sql(_table, engine, if_exists=if_exists, chunksize=chunksize)

            return len(df)

        except SQLAlchemyError as e:
            error = str(e.__dict__['orig'])
            return error
