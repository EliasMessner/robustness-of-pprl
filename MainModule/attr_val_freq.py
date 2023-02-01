import pandas as pd

from random_sample import random_sample_wrapper


def attr_value_distribution_random_sample(df: pd.DataFrame, desired_distr: dict, desired_size: int, attr_name: str,
                                          seed: int = None):
    """
    Draws and returns a random subset with the desired distribution of attribute values.
    desired_distr should be a dict with:
        keys = attribute values in case of categorical attribute
            or ranges (given as tuples) in case of numerical attribute
        values = the portion that each of the attribute values (or ranges) should make up in the drawn sample.
            the portions must add up to 1.
    """
    if isinstance(list(desired_distr.keys())[0], tuple):
        # keys are given as ranges
        condition = lambda key, value: key[0] <= value <= key[1]
    else:
        # keys are given as exact values
        condition = lambda key, value: key == value
        check_all_values_possible(attr_name, desired_distr, df)
    check_portions_sum(desired_distr)
    check_size_possible(df, desired_distr, desired_size, attr_name, condition)
    # draw randomly according to the desired distribution
    result_subsets = [random_sample_wrapper(df[df.apply(lambda row: condition(key, row[attr_name]), axis=1)],
                                            total_sample_size=round(portion * desired_size),
                                            seed=seed)
                      for key, portion in desired_distr.items()]
    # concatenate all and return
    return pd.concat(result_subsets)
# TODO write test cases


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
