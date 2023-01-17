import pandas as pd


def get_true_matches(df_a: pd.DataFrame, df_b: pd.DataFrame, global_id_col_name="globalID") -> (pd.DataFrame, pd.DataFrame):
    """
    Return form each source all records that are part of the overlap, aka all records that have a true match in the
    other source.
    Returned as tuple of two dataframes, one for each sourceID.
    They are not linked to their partners, i.e. the dataframes have no specific order.
    """
    true_matches_a = df_a[df_a[global_id_col_name].isin(df_b[global_id_col_name])]
    true_matches_b = df_b[df_b[global_id_col_name].isin(df_a[global_id_col_name])]
    return true_matches_a, true_matches_b


def get_matching_pairs(df_a: pd.DataFrame, df_b: pd.DataFrame, global_id_col_name="globalID") -> (pd.DataFrame, pd.DataFrame):
    true_matches_a, true_matches_b = get_true_matches(df_a, df_b, global_id_col_name)
    true_matches_a = true_matches_a.sort_values(by=[global_id_col_name])
    true_matches_b = true_matches_b.sort_values(by=[global_id_col_name])


def get_overlap(df_a: pd.DataFrame, df_b: pd.DataFrame, global_id_col_name="globalID") -> float:
    """
    Returns overlap of a given dataset. Overlap is calculated wrt. size of one source,
    which means that if there are two datasets A and B with each 100 records,
    20 of which are matches and 80 non-matches, then the overlap will be 0.2.
    :param df_a: All records in source A
    :param df_b: All records in source B
    :return: Overlap value between 0 and 1.
    :rtype: float
    """
    df = pd.concat([df_a, df_b])
    intersect = pd.merge(df_a, df_b, how="inner", on=[global_id_col_name])
    return 2 * intersect.shape[0] / df.shape[0]


def split_by_source_id(df: pd.DataFrame, source_id_col_name="sourceID", number_of_sources=2) -> list[pd.DataFrame]:
    sources = [x for _, x in df.groupby(source_id_col_name)]
    if number_of_sources is not None and len(sources) != number_of_sources:
        raise ValueError(f"Expected {number_of_sources} sources but found {len(sources)}.")
    return sources


def split_and_get_overlap(df: pd.DataFrame, global_id_col_name="globalID", source_id_col_name="sourceID",
                          number_of_sources=2) -> float:
    df1, df2 = split_by_source_id(df, source_id_col_name, number_of_sources)
    return get_overlap(df1, df2, global_id_col_name)