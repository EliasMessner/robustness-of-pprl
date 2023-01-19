import pandas as pd
import numpy as np
import nltk

from dataset_properties import split_by_source_id, get_true_matches


def get_pairs(df_a: pd.DataFrame, df_b: pd.DataFrame, global_id_col_name="globalID", suffixes=None) -> pd.DataFrame:
    if suffixes is None:
        suffixes = ["_a", "_b"]
    matches_a, matches_b = get_true_matches(df_a, df_b, global_id_col_name)
    return matches_a.merge(matches_b, on=global_id_col_name, suffixes=suffixes)


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


def get_all_errors(df: pd.DataFrame, measure: callable) -> pd.Series:
    df1, df2 = split_by_source_id(df)
    pairs = get_pairs(df1, df2)
    return pairs.apply(measure, axis=1)


def filter_by_error_rate(df: pd.DataFrame, min_e, max_e, measure: callable, global_id_col_name="globalID",
                         source_id_col_name="sourceID", preserve_overlap=False, seed: int = None):
    """
    Filter all matching pairs so that only those with the specified error rate remain.
    If preserve_overlap = True, sample down non matches to the same factor. In that case, a seed can be specified.
    """
    if not preserve_overlap and seed is not None:
        raise ValueError("Seed should only be specified if preserve_overlap = True.")
    if min_e > max_e:
        raise ValueError("min_e must be smaller or equal to max_e")
    df_a, df_b = split_by_source_id(df, source_id_col_name)
    pairs = get_pairs(df_a, df_b)
    pairs["error"] = pairs.apply(measure, axis=1)
    pairs_filtered = pairs[(pairs["error"] >= min_e) &
                           (pairs["error"] <= max_e)]
    matches_a, matches_b = get_true_matches(df_a, df_b, global_id_col_name)
    matches_a_filtered = matches_a[matches_a[global_id_col_name].isin(pairs_filtered[global_id_col_name])]
    matches_b_filtered = matches_b[matches_b[global_id_col_name].isin(pairs_filtered[global_id_col_name])]
    assert matches_a.shape[0] == matches_b.shape[0]
    assert matches_a_filtered.shape[0] == matches_b_filtered.shape[0]
    non_matches = df[~df[global_id_col_name].isin(pairs[global_id_col_name])]
    if preserve_overlap:
        # scale down non-matches to the same factor
        sample_size = round(non_matches.shape[0] * matches_a_filtered.shape[0] / matches_a.shape[0])
        non_matches = non_matches.sample(sample_size, seed)
    return pd.concat([matches_a_filtered, matches_b_filtered, non_matches])
