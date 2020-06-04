import pandas as pd

from tests.dataverk.connectors.databases.test_resources.common import PANDAS_DATAFRAME
from sqlalchemy.exc import SQLAlchemyError, OperationalError


class MockPandas:

    @staticmethod
    def read_sql(
        sql,
        con,
        index_col=None,
        coerce_float=True,
        params=None,
        parse_dates=None,
        columns=None,
        chunksize=None,
        *args,
        **kwargs
    ) -> pd.DataFrame:
        if kwargs.get("raise_sqlalchemy_error"):
            raise SQLAlchemyError("error")
        elif kwargs.get("raise_operational_error"):
            raise OperationalError(statement="error", params={"error": "error"}, orig="error")
        else:
            return PANDAS_DATAFRAME

    @staticmethod
    def to_sql(
        name,
        con,
        schema=None,
        if_exists="fail",
        index=True,
        index_label=None,
        chunksize=None,
        dtype=None,
        method=None,
        *args,
        **kwargs
    ):
        if kwargs.get("raise_sqlalchemy_error"):
            raise SQLAlchemyError("error")
        elif kwargs.get("raise_operational_error"):
            raise OperationalError(statement="error", params={"error": "error"}, orig="error")
        else:
            pass
