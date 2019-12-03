import json
from collections import Sequence, Mapping
from os import environ

import pandas as pd
import requests

from dataverk.exceptions.dataverk_exceptions import EnvironmentVariableNotSet


def anonymize_replace(df, eval_column, anonymize_columns=None, evaluator=lambda x: x < 4, replace_by="*",
                      anonymize_eval=True) -> pd.DataFrame:
    """ Replace values in columns when value in eval_column is less than lower_limit

    :param df: pandas DataFrame
    :param eval_column: name of column to evaluate for anonymization
    :param anonymize_columns: optional, column name or list of column(s) to anonymize if value in eval_column satisfies the
    condition given in evaluator, default=None
    :param evaluator: lambda function, condition for anonymization based on values in eval_column, default=lambda x: x < 4
    :param replace_by: value or list or dict of values to replace by. List or dict passed must have same length as the number
    of columns to anonymize. Elements in list passed should in addition have the same order as columns in

    a) anonymize_columns + eval_columns if anonymize_eval=True and eval_column is _not_ given in anonymize_columns
    b) anonymize_columns                if anonymize_eval=True and eval_column is given in anonymize_columns
                                        or anonymize_eval=False
    c) eval_column                      if anonymize_eval=True and anonymize_columns=False or anonymize_columns=[]

    The order of values to replace by in dictionary does not matter.

    :param anonymize_eval, bool, whether to anonymize eval_column, default=True

    :return: anonymized pandas DataFrame
    """
    return _replace(df, eval_column, anonymize_columns, evaluator, replace_by, anonymize_eval)


def _replace(df: pd.DataFrame, eval_column, anonymize_columns, evaluator, replace_by, anonymize_eval):
    _check_valid_anonymization(anonymize_columns, anonymize_eval)
    columns = _set_columns_to_anonymize(df, eval_column, anonymize_columns, anonymize_eval)

    columns, replace_by = _set_replace_by(df, columns, replace_by)

    to_anonymize = df.copy()
    return _replace_value(to_anonymize, eval_column, columns, evaluator, replace_by)


def _check_valid_anonymization(anonymize_columns, anonymize_eval):
    if anonymize_columns is None and not anonymize_eval:
        raise ValueError("df will not be anonymized, no additional columns are given and anonymize_eval is set to False")


def _set_columns_to_anonymize(df, eval_column, anonymize_columns, anonymize_eval):
    columns = _check_anonymize_columns_input_type(anonymize_columns)

    if anonymize_eval and eval_column not in columns:
        columns += [eval_column]

    if not anonymize_eval and eval_column in columns:
        columns = [col for col in columns if col != eval_column]

    _check_column_names(df, columns)
    return columns


def _check_anonymize_columns_input_type(anonymize_columns):
    if anonymize_columns is None:
        anonymize_columns = []

    elif not isinstance(anonymize_columns, Sequence) or isinstance(anonymize_columns, str):
        anonymize_columns = [anonymize_columns]

    return anonymize_columns


def _check_column_names(df, columns):
    for column in columns:
        if column not in df.columns:
            raise AttributeError(f"'{column}' is not a column in df")


def _set_replace_by(df, columns, replace_by):
    column_order, replace_by_order = columns, replace_by

    if isinstance(replace_by, Mapping):
        replace_in_columns = list(replace_by.keys())
        replace_by_values = list(replace_by.values())

        _check_column_names(df, replace_in_columns)
        column_order, replace_by_order = replace_in_columns, replace_by_values

    return column_order, replace_by_order


def _replace_value(df, eval_column, columns, evaluator, replace_by):
    df.loc[df[df[eval_column].apply(evaluator)].index, columns] = replace_by
    return df


def name_replace(df, columns) -> pd.DataFrame:
    """ Replaces names in columns

    :param df: pandas DataFrame
    :param columns: list of columns to apply name replacement
    :return: pandas DataFrame
    """
    try:
        url = environ["DATAVERK_NAME_REPLACE_API"]
    except KeyError as missing_env:
        raise EnvironmentVariableNotSet(missing_env)

    to_anonymize = df.copy()
    for column in columns:
        res = requests.post(url, data={'values': json.dumps(to_anonymize[column].tolist())})
        filtered_list = json.loads(res.text)['result']
        to_anonymize[column] = pd.Series(filtered_list)
    return to_anonymize
