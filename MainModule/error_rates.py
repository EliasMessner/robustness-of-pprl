import pandas as pd
import numpy as np
import nltk

from dataset_properties import split_by_source_id


def get_pairs(df1: pd.DataFrame, df2: pd.DataFrame, suffixes=None) -> pd.DataFrame:
    if suffixes is None:
        suffixes = ["_a", "_b"]
    matches1 = df1[df1.globalID.isin(df2.globalID)]
    matches2 = df2[df2.globalID.isin(df1.globalID)]
    return matches1.merge(matches2, on="globalID", suffixes=suffixes)


def get_attrs(col_names: list[str]):
    return [col_name for col_name in col_names if not col_name.lower().endswith("id")]


def count_errors(row, attrs, suffixes=None):
    if suffixes is None:
        suffixes = ["_a", "_b"]
    errors = 0
    for attr in attrs:
        if row[attr + suffixes[0]] != row[attr + suffixes[1]]:
            errors += 1
    return errors


def avg_edit_distance(row, attrs, suffixes=None):
    if suffixes is None:
        suffixes = ["_a", "_b"]
    return np.mean(
        [nltk.edit_distance(str(row[attr + suffixes[0]]), str(row[attr + suffixes[1]]))
         for attr in attrs]
    )


def get_all_errors(df: pd.DataFrame, measure: callable, attrs: list[str]) -> pd.Series:
    df1, df2 = split_by_source_id(df)
    pairs = get_pairs(df1, df2)
    return pairs.apply(lambda row: measure(row, attrs), axis=1)