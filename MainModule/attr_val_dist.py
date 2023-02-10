import random

import pandas as pd

from dataset_properties import split_and_get_overlap
from random_sample import random_sample_wrapper


def attr_val_dist_random_sample(df: pd.DataFrame, desired_distr: dict, desired_size: int, attr_name: str,
                                preserve_overlap=False, seed: int = None):
    """
    Draws and returns a random subset with the desired distribution of attribute values.
    desired_distr should be a dict with:
        keys = attribute values in case of categorical attribute
            or ranges (given as tuples) in case of numerical attribute
        values = the portion that each of the attribute values (or ranges) should make up in the drawn sample.
            the portions must add up to 1.
    """
    if seed is None:
        seed = random.randint(0, 10000)
        # need to use same seed for getting the scaling factor and for drawing
        # subsets, to avoid minor deviation of overlap
    if isinstance(list(desired_distr.keys())[0], tuple):
        # keys are given as ranges
        condition = lambda key, value: key[0] <= value <= key[1]
    else:
        # keys are given as exact values
        condition = lambda key, value: key == value
        check_all_values_possible(attr_name, desired_distr, df)
    check_portions_sum(desired_distr)
    check_size_possible(df, desired_distr, desired_size, attr_name, condition)
    scaling_factor = get_scaling_factor(df, desired_distr, desired_size, attr_name, preserve_overlap, seed)
    # draw randomly according to the desired distribution
    result_subsets = []
    for key, portion in desired_distr.items():
        subset_pool = df[df.apply(lambda row: condition(key, row[attr_name]), axis=1)]  # pool to draw subset from
        scaled_overlap = None if not preserve_overlap else split_and_get_overlap(subset_pool) * scaling_factor
        subset = random_sample_wrapper(subset_pool, total_sample_size=round(portion * desired_size),
                                       seed=seed, overlap=scaled_overlap)
        result_subsets.append(subset)
    # concatenate all and return
    return pd.concat(result_subsets)


def get_scaling_factor(df, desired_distr, desired_size, attr_name, preserve_overlap, seed):
    """
    Return the factor to which the overlaps of the individual subsets must be scaled so that the original overlap can be
    preserved.
    Example:
        base data = 50% F (overlap=0.3), 50% M (overlap=0.1)
        -> base overlap = 0.2
        desired_dist = {F: 75%, M: 25%}
        -> altered overlap = 75% * 0.3 + 25% * 0.1 = 0.25
        -> scaling factor = 0.2 / 0.25 = 0.8
    """
    # get the scaling factor
    if not preserve_overlap:
        return 1.0
    altered_overlap = split_and_get_overlap(
        attr_val_dist_random_sample(df, desired_distr, desired_size, attr_name, preserve_overlap=False, seed=seed)
    )
    original_overlap = split_and_get_overlap(df)
    return original_overlap / altered_overlap


def check_all_values_possible(attr_name, desired_distr, df):
    """
    Raises ValueError if there are values specified in the desired distribution which do not occur in the dataframe.
    """
    possible_values = df[attr_name].unique()
    if not all([key in possible_values for key in desired_distr.keys()]):
        impossible_values = [key for key in desired_distr if key not in possible_values]
        raise ValueError("There is at least one value specified that is not present in the data.\n"
                         f"Impossible value(s): {str(impossible_values)}")


def check_portions_sum(desired_distr):
    """
    Raises ValueError if the portions of the desired distribution do not add up to 1.
    """
    if sum(desired_distr.values()) != 1:
        raise ValueError("Portions must add up to 1.")


def check_size_possible(df: pd.DataFrame, desired_distr: dict, desired_size: int, attr_name: str, condition: callable):
    """
    Raises ValueError if the desired size cannot be drawn from the dataframe with the desired distribution (i.e., there
    are not enough samples with these attribute values)
    """
    for key, portion in desired_distr.items():
        if round(portion * desired_size) > df[df.apply(lambda row: condition(key, row[attr_name]), axis=1)].shape[0]:
            raise ValueError(f"Desired size {desired_size} is not possible to draw with desired distribution.")
