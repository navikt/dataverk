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

    def get_pandas_df(self, query, arraysize=100000):
        start_time = time.time()
        self.log(f'Establishing connection to PostgreSQL database: {self._source}')

        try:
            engine = self._create_engine()
            df = pd.read_sql_query(query, engine)
        except OperationalError:
            self.log(f"Updating db credentials")
            self._update_credentials()
            engine = self._create_engine()
            return pd.read_sql_query(query, engine)
        except SQLAlchemyError as e:
            error = str(e.__dict__['orig'])
            return error
        else:
            end_time = time.time()
            self.log(f'{len(df)} records returned in {end_time - start_time} seconds. Query: {query}')
            return df

    def persist_pandas_df(self, table, schema=None, df=None, chunksize=10000, if_exists="replace"):
        self.log(f'Persisting {len(df)} records to table: {table} in PostgreSQL database: {self._source}')
        start_time = time.time()

        try:
            engine = self._create_engine()
            self._set_role(engine)
            df.to_sql(table, engine, if_exists=if_exists, chunksize=chunksize)
        except OperationalError:
            self.log(f"Updating db credentials")
            self._update_credentials()
            engine = self._create_engine()
            self._set_role(engine)
            return df.to_sql(table, engine, if_exists=if_exists, chunksize=chunksize)
        except SQLAlchemyError as e:
            error = str(e.__dict__['orig'])
            return error
        else:
            end_time = time.time()
            self.log(f"Persisted {len(df)} records to table {table} in {end_time - start_time} seconds")
            return len(df)

    def _create_engine(self):
        db = self._connection_string()
        return create_engine(db)

    def _get_role_name(self):
        vault_path = self._vault_path()
        return f"{vault_path.split('/')[-1]}"

    def _set_role(self, engine):
        try:
            query = f"SET ROLE '{self._get_role_name()}'; COMMIT;"
        except KeyError as err:
            self.log(f"""Unable to set role:
                        {err}""")
        else:
            engine.execute(query)

    def _update_credentials(self):
        try:
            vault_path = self._vault_path()
        except KeyError as err:
            self.log(f"""Unable to update postgres credentials:
                      {err}""")
        else:
            conn_string = self._connection_string()
            parsed_url = url.parse_url(conn_string)
            new_credentials = vault_api.get_database_creds(vault_path)
            self._settings["db_connection_strings"][self._source] = conn_string.replace(parsed_url.auth, new_credentials)
