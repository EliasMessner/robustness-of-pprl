import pandas as pd

from dataset_properties import split_by_source_id, get_true_matches, get_overlap


def random_sample(df_a: pd.DataFrame, df_b: pd.DataFrame, total_size: int, seed: int = None, overlap: float = None,
                  global_id_col_name="globalID") -> pd.DataFrame:
    """
    For documentation see DatasetModifier.random_sample
    """
    if overlap is None:
        overlap = get_overlap(df_a, df_b)
    df = pd.concat([df_a, df_b])
    if not (0 <= total_size <= df.shape[0]):
        raise ValueError(f"total_size must be between 0 and {df.shape[0]}, got {total_size} instead.")
    portion_a, portion_b = [_df.shape[0] / (df_a.shape[0] + df_b.shape[0])
                            for _df in (df_a, df_b)]
    size_a, size_b = [round(total_size * portion)
                      for portion in (portion_a, portion_b)]
    size_overlap = round(total_size * overlap / 2)
    assert size_a >= size_overlap <= size_b
    true_matches_a, true_matches_b = get_true_matches(df_a, df_b, global_id_col_name)
    # draw size_overlap records from true matches of source A
    a_matches = true_matches_a.sample(size_overlap, random_state=seed)
    # get the corresponding partners from B
    b_matches = true_matches_b[true_matches_b[global_id_col_name].isin(a_matches[global_id_col_name])]
    # draw size_a - size_overlap from non-matches of source A
    a_non_matches = df_a[~df_a[global_id_col_name].isin(true_matches_a[global_id_col_name])] \
        .sample(size_a - size_overlap, random_state=seed)
    # likewise for B
    b_non_matches = df_b[~df_b[global_id_col_name].isin(true_matches_b[global_id_col_name])] \
        .sample(size_b - size_overlap, random_state=seed)
    # concatenate all
    return pd.concat([a_matches, a_non_matches, b_matches, b_non_matches])


def random_sample_wrapper(df: pd.DataFrame, total_sample_size: int, seed: int = None, overlap: float = None,
                          global_id_col_name="globalID") -> pd.DataFrame:
    """
    Wrapper function that splits the passed data into the two sources before calling random_sample, allowing to pass
    the whole dataset.
    """
    df_a, df_b = split_by_source_id(df)
    return random_sample(df_a, df_b, total_sample_size, seed, overlap, global_id_col_name)