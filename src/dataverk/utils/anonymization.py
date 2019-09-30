import json
from os import environ
import pandas as pd
import requests


def anonymize_replace(df, eval_column, additional_columns, lower_limit) -> pd.DataFrame:
    """ Replace values in columns with "*" when the value is less than lower_limit

    :param df: pandas DataFrame
    :param eval_column: column to evaluate for anonymization
    :param additional_columns: list of columns to anonymize if value in eval_column is below lower_limit
    :param lower_limit: lower limit for value replacement in data set column
    :return: anonymized pandas DataFrame
    """
    return _replace(df, eval_column, additional_columns, lower_limit)


def _replace(df: pd.DataFrame, eval_column, additional_columns, lower_limit):
    to_anonymize = df.copy()

    if isinstance(additional_columns, str):
        additional_columns = [additional_columns]

    columns = additional_columns
    if eval_column not in additional_columns:
        columns += [eval_column]

    for index, row in to_anonymize.iterrows():
        if row[eval_column] < lower_limit:
            for column in columns:
                to_anonymize.loc[index, column] = "*"
    return to_anonymize


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
