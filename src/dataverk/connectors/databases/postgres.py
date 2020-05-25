import time
import pandas as pd

from sqlalchemy.exc import SQLAlchemyError, OperationalError
from collections.abc import Mapping
from dataverk.connectors.databases.base import DBBaseConnector
from dataverk.connectors.databases.utils.error_strategies import (
    OperationalErrorStrategy,
    ErrorStrategy,
)


class PostgresConnector(DBBaseConnector):
    def __init__(
        self,
        settings_store: Mapping,
        source: str,
        error_strategy: ErrorStrategy = OperationalErrorStrategy(),
    ):
        super().__init__(settings_store, source)
        self.error_strategy = error_strategy

    def get_pandas_df(
        self, query: str, verbose_output: bool = False, *args, **kwargs
    ) -> pd.DataFrame:
        start_time = time.time()
        self.log.info(f"Reading from PostgreSQL database: {self.source}")

        try:
            df = pd.read_sql_query(query, self._engine, *args, **kwargs)
        except OperationalError:
            self.error_strategy.handle_error(self)
            df = pd.read_sql_query(query, self._engine)
        except SQLAlchemyError as error:
            self.log.error(f"{error.__dict__['orig']}")
            raise SQLAlchemyError(f"{error}")

        end_time = time.time()
        self.log.info(f"{len(df)} records returned in {end_time - start_time} seconds.")
        if verbose_output:
            self.log(f"Query: {query}")

        return df

    def persist_pandas_df(self, table: str, df: pd.DataFrame, *args, **kwargs) -> None:
        self.log.info(
            f"Persisting {len(df)} records to table: {table} in PostgreSQL database: {self.source}"
        )
        start_time = time.time()

        try:
            self._set_role()
            df.to_sql(table, self._engine, *args, **kwargs)
        except OperationalError:
            self.error_strategy.handle_error(self)
            self._set_role()
            df.to_sql(table, self._engine, *args, **kwargs)
        except SQLAlchemyError as error:
            self.log.error(f"{error.__dict__['orig']}")
            raise SQLAlchemyError(f"{error}")

        end_time = time.time()
        self.log.info(
            f"Persisted {len(df)} records to table {table} in {end_time - start_time} seconds"
        )

    def _get_role_name(self) -> str:
        vault_path = self.settings["db_vault_path"][self.source]
        return f"{vault_path.split('/')[-1]}"

    def _set_role(self) -> None:
        try:
            query = f"SET ROLE '{self._get_role_name()}'; COMMIT;"
        except KeyError as err:
            self.log.error(f"Unable to set role: {err}")
        else:
            self._engine.execute(query)