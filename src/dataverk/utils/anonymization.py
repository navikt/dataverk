import json
from os import environ
import pandas as pd
import requests


def anonymize_replace(df, columns, lower_limit) -> pd.DataFrame:
    """ Replace values in columns with NaN when the value is less than lower_limit

    :param df: pandas Dataframe
    :param columns: list of columns to apply value replacement
    :param lower_limit: lower limit for value replacement in dataset column
    :return: anonymized pandas Dataframe
    """
    return _replace(df, columns, lower_limit)


def _replace(df: pd.DataFrame, columns: [], lower_limit):
    for column in columns:
        current_col = df[column].tolist()
        current_col = [_replace_value(value, lower_limit) for value in current_col]
        df[column] = current_col
    return df


def _replace_value(value, limit):
    if value < limit:
        return None
    else:
        return value


def name_replace(df, columns) -> pd.DataFrame:
    """ Replaces names in columns

    :param df: pandas DataFrame
    :param columns: list of columns to apply name replacement
    :return: pandas DataFrame
    """
    try:
        url = environ["DATAVERK_NAME_REPLACE_API"]
    except KeyError:
        raise EnvironmentError("DATAVERK_NAME_REPLACE_API env is not set")

    for column in columns:
        res = requests.post(url, data={'values': json.dumps(df[column].tolist())})
        filtered_list = json.loads(res.text)['result']
        df[column] = pd.Series(filtered_list)
    return df
