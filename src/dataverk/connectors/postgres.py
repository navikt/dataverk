import time
import pandas as pd
from urllib3.util import url
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from collections.abc import Mapping
from dataverk.connectors.abc.db_base import DBBaseConnector
from dataverk_vault import api as vault_api


class PostgresConnector(DBBaseConnector):

    def __init__(self, settings_store: Mapping, source=None):
        super().__init__()

        self._settings = settings_store
        self._source = source

    def __enter__(self):
        self._engine = self._create_engine()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._engine.dispose()

    def get_pandas_df(self, query, arraysize=100000, verbose_output=False):
        start_time = time.time()
        self.log(f'Reading from PostgreSQL database: {self._source}')

        try:
            df = pd.read_sql_query(query, self._engine)
        except OperationalError:
            self._reset_db_connection()
            df = pd.read_sql_query(query, self._engine)
        except SQLAlchemyError as e:
            error = str(e.__dict__['orig'])
            return error

        end_time = time.time()
        self.log(f'{len(df)} records returned in {end_time - start_time} seconds.')
        if verbose_output:
            self.log(f'Query: {query}')

        return df

    def persist_pandas_df(self, table, schema=None, df=None, chunksize=10000, if_exists="replace"):
        self.log(f'Persisting {len(df)} records to table: {table} in PostgreSQL database: {self._source}')
        start_time = time.time()

        try:
            self._set_role()
            df.to_sql(table, self._engine, if_exists=if_exists, chunksize=chunksize)
        except OperationalError:
            self._reset_db_connection()
            self._set_role()
            df.to_sql(table, self._engine, if_exists=if_exists, chunksize=chunksize)
        except SQLAlchemyError as e:
            error = str(e.__dict__['orig'])
            return error

        end_time = time.time()
        self.log(f"Persisted {len(df)} records to table {table} in {end_time - start_time} seconds")
        return len(df)

    def _connection_string(self):
        try:
            return self._settings["db_connection_strings"][self._source]
        except KeyError:
            raise KeyError(f"Database connection string not found in settings file."
                           f"Unable to establish connection to PostgreSQL database: {self._source}")

    def _vault_path(self):
        try:
            return self._settings["db_vault_path"][self._source]
        except KeyError:
            raise KeyError(f"db_vault_path for {self._source} not found in settings file."
                           f"Unable to establish connection to PostgreSQL database: {self._source}")

    def _create_engine(self):
        db = self._connection_string()
        return create_engine(db)

    def _get_role_name(self):
        vault_path = self._vault_path()
        return f"{vault_path.split('/')[-1]}"

    def _set_role(self):
        try:
            query = f"SET ROLE '{self._get_role_name()}'; COMMIT;"
        except KeyError as err:
            self.log(f"""Unable to set role:
                        {err}""")
        else:
            self._engine.execute(query)

    def _reset_db_connection(self):
        self._engine.dispose()

        try:
            vault_path = self._vault_path()
        except KeyError as err:
            self.log(f"""Unable to update postgres credentials:
                      {err}""")
        else:
            self._update_credentials(vault_path)
            self._engine = self._create_engine()

    def _update_credentials(self, vault_path):
        self.log(f"Updating db credentials")
        conn_string = self._connection_string()
        parsed_url = url.parse_url(conn_string)
        new_credentials = vault_api.get_database_creds(vault_path)
        self._settings["db_connection_strings"][self._source] = conn_string.replace(parsed_url.auth, new_credentials)
