from typing import Union

import numpy as np
import nltk
import pandas as pd

from dataset_properties import split_by_source_id


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


def get_all_errors(df: pd.DataFrame, measure: Union[callable, str], global_id_col_name="globalID") -> pd.Series:
    measure = globals()[measure] if isinstance(measure, str) else measure  # resolve method name if given as string
    df_a, df_b = split_by_source_id(df)
    pairs = df_a.merge(df_b, on=global_id_col_name, suffixes=["_a", "_b"])
    attrs = [col for col in df.columns.values if not col.lower().endswith("id")]
    return pairs.apply(lambda row: measure(row, attrs), axis=1)
