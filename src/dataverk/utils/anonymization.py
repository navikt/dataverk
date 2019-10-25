import json
from os import environ
import pandas as pd
import requests


def anonymize_replace(df, eval_column, additional_columns=None, lower_limit=4, replace_by="*") -> pd.DataFrame:
    """ Replace values in columns when value in eval_column is less than lower_limit

    :param df: pandas DataFrame
    :param eval_column: str, column to evaluate for anonymization
    :param additional_columns: str or list of columns to anonymize if value in eval_column is below lower_limit, default=None
    :param lower_limit: int, lower limit for value replacement in data set column, default=4
    :param replace_by: str, list or dict, value to replace by. List or dict passed must have same length as number of
                       columns to anonymize. Values in list must also be given in same order as [additional_columns]+[eval_column]


    :return: anonymized pandas DataFrame
    """
    return _replace(df, eval_column, additional_columns, lower_limit, replace_by)


def _replace(df: pd.DataFrame, eval_column: str, additional_columns, lower_limit, replace_by):

    columns = _set_columns_to_anonymize(df, eval_column, additional_columns)

    _check_value_types(df, eval_column, lower_limit)
    columns, replace_by = _check_replace_by(columns, replace_by)

    to_anonymize = df.copy()
    return _replace_value(to_anonymize, eval_column, columns, lower_limit, replace_by)


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
        if not isinstance(column, str):
            raise TypeError(f"{column}: column names should be of type str")
        if column not in df.columns:
            raise ValueError(f"'{column}' is not a column in df")


def _check_value_types(df, eval_column, lower_limit):
    if df[eval_column].dtype not in ["int64", "float"]:
        raise TypeError("Values that are evaluated for anonymization should be of type int or float")

    if not isinstance(lower_limit, (int, float)):
        raise TypeError("lower_limit should be of type int or float")


def _check_replace_by(columns, replace_by):
    column_order, replace_by_order = columns, replace_by

    if not isinstance(replace_by, (int, float, type(None), str, list, dict)):
        raise TypeError("values to replace by should be given as types int, float, None or str or in list or dict")

    if isinstance(replace_by, list):
        if len(replace_by) != len(columns):
            raise Exception("number of replacement values in list of values to replace_by is different from number of columns to anonymize")
        for replace_value in replace_by:
            if not isinstance(replace_value, (int, float, type(None), str)):
                raise TypeError("values in list of values to replace by should be given as types int, float, None or str")

    if isinstance(replace_by, dict):
        replace_in_columns = list(replace_by.keys())
        replace_by_values = list(replace_by.values())

        if len(replace_in_columns) != len(columns):
            raise Exception("replace_by dictionary should have the same number of keys as columns to anonymize")

        for r_column in replace_in_columns:
            if not isinstance(r_column, str):
                raise TypeError(f"{r_column}: keys in replace_by dict should be of type str")
            if r_column not in columns:
                raise ValueError(f"{r_column} in replace_by dictionary not in columns to anonymize")

        for replacement_value in replace_by_values:
            if not isinstance(replacement_value, (int, float, type(None), str)):
                raise TypeError("values in replace_by dictionary should be of types int, float, None or str")

        column_order, replace_by_order = replace_in_columns, replace_by_values

    return column_order, replace_by_order


def _replace_value(df, eval_column, columns, lower_limit, replace_by):
    df.loc[df[df[eval_column] < lower_limit].index, columns] = replace_by
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
