import json
from os import environ
import pandas as pd
import requests


def anonymize_replace(df, eval_column, additional_columns=None, lower_limit=4) -> pd.DataFrame:
    """ Replace values in columns with "*" when the value is less than lower_limit

    :param df: pandas DataFrame
    :param eval_column: column to evaluate for anonymization
    :param additional_columns: list of columns to anonymize if value in eval_column is below lower_limit
    :param lower_limit: lower limit for value replacement in data set column
    :return: anonymized pandas DataFrame
    """
    return _replace(df, eval_column, additional_columns, lower_limit)


def _replace(df: pd.DataFrame, eval_column: str, additional_columns, lower_limit):

    columns = _set_columns_to_anonymize(df, eval_column, additional_columns)

    _check_value_types(df, eval_column, lower_limit)

    to_anonymize = df.copy()
    return _replace_value(to_anonymize, eval_column, columns, lower_limit)


def _set_columns_to_anonymize(df, eval_column, additional_columns):
    columns = _check_additional_columns_type(additional_columns)

    if eval_column not in columns:
        columns += [eval_column]

    _check_column_names(df, columns)
    return columns


def _check_additional_columns_type(additional_columns):
    if additional_columns is None:
        additional_columns = []

    elif isinstance(additional_columns, str):
        additional_columns = [additional_columns]

    elif not isinstance(additional_columns, list):
        raise TypeError("additional_columns should either be string or list containing column name(s)")

    return additional_columns


def _check_column_names(df, columns):
    for column in columns:
        if column not in df.columns:
            raise ValueError(f"\'{column}\' is not a column in df")


def _check_value_types(df, eval_column, lower_limit):
    if df[eval_column].dtype not in ["int64", "float"]:
        raise TypeError("Values that are evaluated for anonymization should be of type int or float")

    if not isinstance(lower_limit, (int, float)):
        raise TypeError("lower_limit should be of type int or float")


def _replace_value(df, eval_column, columns, lower_limit):
    df.loc[df[df[eval_column] < lower_limit].index, columns] = "*"
    return df


def name_replace(df, columns) -> pd.DataFrame:
    """ Replaces names in columns

    :param df: pandas DataFrame
    :param columns: list of columns to apply name replacement
    :return: pandas DataFrame
    """
    to_anonymize = df.copy()
    try:
        url = environ["DATAVERK_NAME_REPLACE_API"]
    except KeyError:
        raise EnvironmentError("DATAVERK_NAME_REPLACE_API env is not set")

    for column in columns:
        res = requests.post(url, data={'values': json.dumps(to_anonymize[column].tolist())})
        filtered_list = json.loads(res.text)['result']
        to_anonymize[column] = pd.Series(filtered_list)
    return to_anonymize
