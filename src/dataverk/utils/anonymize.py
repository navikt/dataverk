import pandas as pd


def replace(df: pd.DataFrame, columns: [], lower_limit):
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
