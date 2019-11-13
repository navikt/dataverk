import json
from collections import Sequence, Mapping
from os import environ
from typing import Callable

import pandas as pd
import requests


def anonymize_replace(df, eval_column, anonymize_columns, evaluator=lambda x: x<4, replace_by="*") -> pd.DataFrame:
    """ Replace values in columns when value in eval_column is less than lower_limit

    :param df: pandas DataFrame
    :param eval_column: name of column to evaluate for anonymization
    :param anonymize_columns: optional, column name or list of column(s) to anonymize if value in eval_column is below
    lower_limit, default=None
    :param lower_limit: int or float, lower limit for value replacement in data set column, default=4
    :param replace_by: value, list or dict of values to replace by. List or dict passed must have same length as number
    of columns to anonymize. Values in list must be given in same order as [additional_columns]+[eval_column]

    :return: anonymized pandas DataFrame
    """
    return _replace(df, eval_column, anonymize_columns, evaluator, replace_by)


def anonymize_replace_by_label(df, eval_column, labels, additional_columns=None, replace_by="*",
                               anonymize_eval=True) -> pd.DataFrame:
    """ Replace values in columns when value in eval_column given label or in label list

    :param df: pandas DataFrame
    :param eval_column: name of column to evaluate for anonymization
    :param labels: int, float, str or NoneType or list, label(s) for value replacement in data set column, default=None.
    :param additional_columns: optional, column name or list of column(s) to anonymize if value in eval_column is given
    in 'label', default=None
    :param replace_by: value or list or dict of values to replace by. List or dict passed must have same length as
    number of columns to anonymize. Values in list must be given in same order as [additional_columns]+[eval_column]
    :param anonymize_eval: bool, whether eval_column should be anonymized, default=True

    :return: anonymized pandas DataFrame
    """
    return _replace_by_label(df, eval_column, additional_columns, labels, replace_by, anonymize_eval)


def _replace(df: pd.DataFrame, eval_column, additional_columns, evaluator, replace_by):
    columns = _set_columns_to_anonymize(df, eval_column, additional_columns)

    columns, replace_by = _set_replace_by(df, columns, replace_by)

    to_anonymize = df.copy()
    return _replace_value(to_anonymize, eval_column, columns, evaluator, replace_by)


def _replace_by_label(df: pd.DataFrame, eval_column, additional_columns, labels, replace_by, anonymize_eval):
    _check_valid_anonymization_by_label(additional_columns, anonymize_eval)

    columns = _set_columns_to_anonymize(df, eval_column, additional_columns, anonymize_eval)
    labels = _set_labels(df, eval_column, labels)
    columns, replace_by = _set_replace_by(df, columns, replace_by)

    to_anonymize = df.copy()
    return _replace_label(to_anonymize, eval_column, columns, labels, replace_by)


def _check_valid_anonymization_by_label(additional_columns, anonymize_eval):
    if additional_columns is None and not anonymize_eval:
        raise ValueError("df will not be anonymized, no additional columns are given and anonymize_eval is set to False")


def _set_columns_to_anonymize(df, eval_column, additional_columns, anonymize_eval=True):
    columns = _check_additional_columns_input_type(additional_columns)

    if anonymize_eval and eval_column not in columns:
        columns += [eval_column]

    _check_column_names(df, columns)
    return columns


def _check_additional_columns_input_type(additional_columns):
    if additional_columns is None:
        additional_columns = []

    elif not isinstance(additional_columns, Sequence):
        additional_columns = [additional_columns]

    return additional_columns


def _check_column_names(df, columns):
    for column in columns:
        if column not in df.columns:
            raise AttributeError(f"'{column}' is not a column in df")


def _set_labels(df, eval_column, labels):
    if df[eval_column].dtype not in ["int64", "float64", "object"]:
        raise TypeError("Labels that are evaluated for anonymization should be of type int, float or str")

    if isinstance(labels, (int, float, str, type(None))):
        labels = [labels]

    elif isinstance(labels, list):
        for label in labels:
            if not isinstance(label, (int, float, str, type(None))):
                raise TypeError(f"{label}: labels in label list should be of type int, float, str, or NoneType")

    else:
        raise TypeError("input 'label' should be of type int, float, str or list")

    return labels


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


def _replace_label(df, eval_column, columns, labels, replace_by):
    df.loc[df[eval_column].isin(labels), columns] = replace_by
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

    to_anonymize = df.copy()
    for column in columns:
        res = requests.post(url, data={'values': json.dumps(to_anonymize[column].tolist())})
        filtered_list = json.loads(res.text)['result']
        to_anonymize[column] = pd.Series(filtered_list)
    return to_anonymize
