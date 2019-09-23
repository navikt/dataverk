import json
from os import environ
import pandas as pd
import requests


def anonymize_replace(df, eval_column, additional_columns, lower_limit) -> pd.DataFrame:
    """ Replace values in columns with NaN when the value is less than lower_limit

    :param eval_column: column to evaluate for anonymization
    :param additional_columns: list of columns to anonymize if value in eval_column is below lower_limit
    :param df: pandas Dataframe
    :param lower_limit: lower limit for value replacement in dataset column
    :return: anonymized pandas Dataframe
    """
    return _replace(df, eval_column, additional_columns, lower_limit)


def _replace(df: pd.DataFrame, eval_column, additional_columns: [], lower_limit):
    columns = additional_columns
    if eval_column not in additional_columns:
        columns += [eval_column]

    for index, row in df.iterrows():
        if row[eval_column] < lower_limit:
            for column in columns:
                df.loc[index, column] = "*"
    return df


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
