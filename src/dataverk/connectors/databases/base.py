import time
import pandas as pd

from typing import Mapping
from sqlalchemy import engine
from sqlalchemy.exc import SQLAlchemyError
from dataverk.abc.base import DataverkBase


class DBBaseConnector(DataverkBase):
    def __init__(self, settings_store: Mapping, source: str):
        super().__init__()
        self.settings = settings_store
        self.source = source

    def __enter__(self):
        self._engine = self._create_engine()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._engine.dispose()

    def get_pandas_df(
        self, query: str, verbose_output: bool = False, *args, **kwargs
    ) -> pd.DataFrame:
        start_time = time.time()
        self.log.info(f"Reading from database: {self.source}")

        df = pd.read_sql(query, self._engine, *args, **kwargs)

        end_time = time.time()
        self.log.info(f"{len(df)} records returned in {end_time - start_time} seconds.")
        if verbose_output:
            self.log.info(f"Query: {query}")

        return df

    def persist_pandas_df(self, table: str, df: pd.DataFrame, *args, **kwargs) -> None:
        start_time = time.time()
        self.log.info(
            f"Persisting {len(df)} records to table: {table} in database: {self.source}"
        )

        df.to_sql(name=table, con=self._engine, *args, **kwargs)

        end_time = time.time()
        self.log.info(
            f"Persisted {len(df)} records to table {table} in {end_time - start_time} seconds"
        )

    def execute_sql(self, query: str, verbose_output: bool = False, *args, **kwargs):
        start_time = time.time()
        self.log.info(
            f"Executing sql query in database: {self.source}"
        )

        self._engine.execute(query)

        end_time = time.time()

        self.log.info(f"Executed query in {end_time -start_time} seconds.")
        if verbose_output:
            self.log.info(f"Query: {query}")

    def _create_engine(self) -> engine.Engine:
        db = self._connection_string()
        return engine.create_engine(db)

    def _connection_string(self) -> str:
        return self.settings["db_connection_strings"][self.source]
