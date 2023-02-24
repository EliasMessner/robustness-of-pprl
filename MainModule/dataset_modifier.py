import logging
import os.path
import shutil
from ast import literal_eval
from pathlib import Path
from typing import Union

import pandas as pd
from tqdm import tqdm
import itertools
from datetime import datetime as dt

import error_rates
from dataset_properties import get_overlap, get_true_matches, split_by_source_id
from attr_val_dist import attr_val_dist_random_sample
from random_sample import random_sample, random_sample_wrapper
from util import read_json, write_json, get_config_path_from_argv
from constants import default_dm_config_path, dataset_variants_dir, logs_dir


def main():
    dm_config_path = get_config_path_from_argv(default=default_dm_config_path)
    print(f"Creating variants based on {dm_config_path}")
    # delete existing dataset variants
    shutil.rmtree(dataset_variants_dir, ignore_errors=True)
    Path(dataset_variants_dir).mkdir(exist_ok=True)
    # store dm_config.json at top level in dataset_variants folder, so it can be logged by mlflow later
    shutil.copyfile(dm_config_path, os.path.join(dataset_variants_dir, "dm_config.json"))
    # create dataset variations
    dm = DatasetModifier()
    dm.load_dataset_by_config_file(dm_config_path)
    dm.create_variants_by_config_file(dm_config_path, dataset_variants_dir)


def prepare_logger():
    logs_sub_dir = os.path.join(logs_dir, "dataset_modifier")
    Path(logs_sub_dir).mkdir(parents=True, exist_ok=True)
    timestamp = dt.now().strftime("%Y-%m-%d_%H-%M-%S")
    logging.basicConfig(filename=os.path.join(logs_sub_dir, f"{timestamp}.txt"),
                        filemode='a',
                        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.INFO)
    logging.getLogger().addHandler(logging.StreamHandler())


class DatasetModifier:
    def __init__(self, omit_if_not_possible=True, omit_if_too_small=True, min_size_per_source=10):
        """
        :param omit_if_not_possible: Set to True (default) if a variant should be omitted instead of raising a
        ValueError, in case a specified ds-variant cannot be drawn (for example, a random sample with an overlap too large)
        """
        self.omit_if_not_possible = omit_if_not_possible
        self.omit_if_too_small = omit_if_too_small
        self.omitted_too_small = 0
        self.omitted_invalid_params = 0
        self.min_size_per_source = min_size_per_source
        self.df = None
        self.df_a = None
        self.df_b = None
        self.source_id_col_name = None
        self.global_id_col_name = None
        self.true_matches_a = None
        self.true_matches_b = None
        self.base_overlap = None
        self.attrs = None  # all attributes that are not IDs, such as name, address, etc
        self.pairs = None  # merged version of the true matches a and b

    def load_dataset_by_config_file(self, config_path: str):
        """
        Reads config file (json) and loads dataset as specified in the file.
        More precisely, reads parameters from json and calls self.read_csv with the parameters.
        """
        config = read_json(config_path)
        self.read_csv_config_dict(config)

    def read_csv_config_dict(self, config: dict):
        """
        Calls self.read_csv and accepts parameters as a dict.
        """
        self.read_csv(dataset_path=config["base_dataset"], col_names=config["col_names"],
                      source_id_col_name=config["source_id_col_name"], global_id_col_name=config["global_id_col_name"])

    def read_csv(self, dataset_path: str, col_names: list, source_id_col_name: str = "sourceID",
                 global_id_col_name: str = "globalID"):
        """
        The passed dataset is expected to consist of two equally sized sources,
        distinguishable by a column containing a sourceID.
        :param dataset_path: Filepath to dataset csv
        :type dataset_path: str
        :param col_names: column names of dataset
        :type col_names: list of str
        :param source_id_col_name: name of the column containing the two sourceID (e.g. A and B), defaults to "sourceID"
        :type source_id_col_name: str
        :param global_id_col_name: name of column containing global ID, defaults to "globalID"
        :type global_id_col_name: str
        """
        self.df = pd.read_csv(dataset_path, names=col_names, dtype={"PLZ": str},
                              keep_default_na=False)  # keep_default_na for representing empty PLZ values as empty str and not nan
        self.df_a, self.df_b = split_by_source_id(self.df)
        assert self.df_a.shape == self.df_b.shape
        self.global_id_col_name = global_id_col_name
        self.source_id_col_name = source_id_col_name
        self.true_matches_a, self.true_matches_b = get_true_matches(self.df_a, self.df_b, self.global_id_col_name)
        self.base_overlap = get_overlap(self.df_a, self.df_b, self.global_id_col_name)
        self.attrs = [col for col in self.df.columns.values if not col.lower().endswith("id")]
        self.pairs = self.true_matches_a.merge(self.true_matches_b, on=self.global_id_col_name, suffixes=["_a", "_b"])

    def create_variants_by_config_file(self, config_path, outfile_directory):
        """
        Read dataset modifier config file, create dataset variations as described in the file, write them all to
        out_location folder.
        For each "variation" value in the config file, a separate sub-folder (dv group) is created.
        In the group sub folder, each variant goes in a sub folder containing its parameters in params.json and its
        records in records.csv.
        """
        param_variant_groups = get_param_variant_groups(read_json(config_path))
        Path(outfile_directory).mkdir(exist_ok=True)
        group_id = 0
        for param_variant_group, description in tqdm(param_variant_groups, desc="Groups"):
            # Downsampling must be done separately,
            # when all ds variants have been created,
            # because the minimum ds size in the group must be known first.
            variant_group = self.get_variant_group(param_variant_group)
            variant_group = sample_all_down_if_needed(variant_group)
            write_group_folder(variant_group, group_id, description, outfile_directory)
            group_id += 1
        self.log_and_reset_omitted()

    def get_variant_group(self, param_variants):
        """
        Return a list of variants from a given list of parameter combinations.
        """
        variants = []
        for params in param_variants:
            variant = self.get_variant(params)
            if variant is None:
                # variant could be None if for example a ValueError occurred due to impossible sample size in random
                #  subset
                self.omitted_invalid_params += 1
                continue
            if self.check_if_variant_should_be_omitted(variant):
                self.omitted_too_small += 1
                continue
            variants.append((variant, params))
        return variants

    def get_variant(self, params) -> pd.DataFrame:
        """
        Does not apply down-sampling because minimum dataset size in group must be known for that.
        """
        if params["subset_selection"] == "RANDOM":
            return self.random_sample(params)
        if params["subset_selection"] == "ATTRIBUTE_VALUE":
            return self.attribute_value_subset(params)
        if params["subset_selection"] == "PLZ":
            return self.plz_subset(params)
        if params["subset_selection"] == "AGE":
            return self.age_subset(params)
        if params["subset_selection"] == "ERROR_RATE":
            return self.error_rate_subset(params)

    def check_if_variant_should_be_omitted(self, variant):
        if not self.omit_if_too_small:
            return False
        sources = split_by_source_id(variant, number_of_sources=None)
        if len(sources) < 2:
            return True
        a, b = sources
        if min(a.shape[0], b.shape[0]) < self.min_size_per_source:
            return True
        return False

    def random_sample(self, params) -> Union[pd.DataFrame, None]:
        """
        Draw random sample from base dataset.
        If the desired overlap cannot be drawn, ...
            ...returns None, if self.omit_if_not_possible == True
            ...raises a ValueError, otherwise
        :param params: dict containing the keys described below.
        size (int): number of records to draw all together (the ratio between source A and B will be preserved as
        closely as possible after rounding)
        seed (int): Seed for reproducibility
        overlap (float) (optional): (2* number of matching pairs) / (total data size), if not specified the ratio will
        be the same as in the base dataset
        :return: random sample drawn from base dataset
        """
        try:
            return random_sample(self.df_a, self.df_b, total_size=params["size"], seed=params["seed"],
                                 overlap=params.get("overlap", None))
        except ValueError as e:
            if self.omit_if_not_possible:
                logging.warning(f"Could not draw random sample because ValueError was raised.\n"
                                f"Omitting parameters: \n"
                                f"{params} \n"
                                f"Exception: {e}")
                return None
            else:
                raise ValueError(f"{e}\nParameters causing ValueError: {params}")

    def attribute_value_subset(self, params: dict) -> pd.DataFrame:
        assert len({"equals", "is_in", "range", "dist",
                    "length"} & params.keys()) == 1  # exactly one of these keys must be present
        if "equals" in params:
            return self.df[self.df[params["column"]] == params["equals"]]
        if "is_in" in params:
            return self.df[self.df[params["column"]].map(lambda value: value in params["is_in"])]
        if "range" in params:
            min_v = params["range"][0]
            max_v = params["range"][1]
            return self.df[self.df[params["column"]].map(lambda value: min_v <= value <= max_v)]
        if "dist" in params:
            dist = get_dist_as_dict(params)
            return attr_val_dist_random_sample(self.df,
                                               desired_dist=dist,
                                               desired_size=params.get("size", None),
                                               attr_name=params["column"],
                                               is_range=params.get("dist_is_range", False),
                                               preserve_overlap=params.get("preserve_overlap", False),
                                               seed=params.get("seed", None))
        if "length" in params:
            return self.filter_by_attr_value_length(params)

    def filter_by_attr_value_length(self, params):
        min_len, max_len = params["length"]
        mask = (self.df[params["column"]].str.len() >= min_len) & (self.df[params["column"]].str.len() <= max_len)
        return self.df[mask]

    def plz_subset(self, params) -> pd.DataFrame:
        n = params["digits"]  # first n digits
        value = params["equals"]  # must be equal to value
        return self.df[self.df["PLZ"].map(lambda plz: plz.isdigit() and int(plz[0:n]) == value)]

    def age_subset(self, params) -> pd.DataFrame:
        this_year = dt.now().year
        min_age = params["range"][0]
        max_age = params["range"][1]
        return self.df[self.df["YEAROFBIRTH"].map(lambda yob: min_age <= (this_year - yob) <= max_age)]

    def error_rate_subset(self, params):
        min_e, max_e = params["range"]
        return self.filter_by_error_rate(min_e, max_e, params["measure"],
                                         params.get("preserve_overlap", False), params.get("seed", None))

    def filter_by_error_rate(self, min_e, max_e, measure: Union[callable, str], preserve_overlap=False,
                             seed: int = None):
        """
        Filter all matching pairs so that only those with the specified error rate remain.
        If preserve_overlap = True, sample down non matches to the same factor. In that case, a seed can be specified.
        """
        if min_e > max_e:
            raise ValueError("min_e must be smaller or equal to max_e")
        # resolve method name if given as string
        measure = getattr(error_rates, measure) if isinstance(measure, str) else measure
        error_column = measure.__name__  # name of the column where the error for each pair should be stored
        if error_column not in self.pairs.columns:
            self.pairs[error_column] = self.pairs.apply(lambda row: measure(row, self.attrs), axis=1)
        pairs_filtered = self.pairs[(self.pairs[error_column] >= min_e) &
                                    (self.pairs[error_column] <= max_e)]
        matches_a_filtered = self.true_matches_a[
            self.true_matches_a[self.global_id_col_name].isin(pairs_filtered[self.global_id_col_name])]
        matches_b_filtered = self.true_matches_b[
            self.true_matches_b[self.global_id_col_name].isin(pairs_filtered[self.global_id_col_name])]
        assert matches_a_filtered.shape[0] == matches_b_filtered.shape[0]
        non_matches = self.df[~self.df[self.global_id_col_name].isin(self.pairs[self.global_id_col_name])]
        if preserve_overlap:
            # scale down non-matches to the same factor
            sample_size = round(non_matches.shape[0] * matches_a_filtered.shape[0] / self.true_matches_a.shape[0])
            non_matches = non_matches.sample(n=sample_size, random_state=seed)
        return pd.concat([matches_a_filtered, matches_b_filtered, non_matches])

    def log_and_reset_omitted(self):
        if self.omitted_too_small:
            logging.info(
                f"Omitted {self.omitted_too_small} variants because they were smaller than min_size_per_source")
        if self.omitted_invalid_params:
            logging.info(
                f"Omitted {self.omitted_invalid_params} variants because they could not be created (possibly due to "
                f"impossible parameter combination). See logs for further info.")


def get_param_variant_groups(config) -> list[(list[dict], str)]:
    """
    Return a grouped list of all parameter variations created from given config dict.
    All parameter variations created from the same "variations" key, are contained in the same group.
    If variation lists for multiple parameters are given in one replacement, they
    are combined to their cartesian product.
    For each parameter variation, a tuple containing (variant, comment) is returned, comment being the comment in the
    config file describing this param variation, or an empty string if no comment was specified.
    """
    groups = []
    for variation in config["variations"]:
        param_vars = []
        description = variation.get("desc", "")
        params = variation["params"]
        replacements = variation.get("replacements", {})
        if variation.get("as_range", False):
            _handle_ranges(replacements)
        cartesian_prod = _get_cartesian_product(replacements.items())
        for kv_combination in cartesian_prod:
            param_variation = _get_param_variation(kv_combination, params)
            param_vars.append(param_variation)
        if variation.get("include_default", False):
            param_vars.append(params)
        groups.append((param_vars, description))
    return groups


def _handle_ranges(replacements):
    for k, v in replacements.items():
        if isinstance(v, list):
            replacements[k] = range(*v)


def _get_param_variation(kv_combination: list[(str, any)], params: dict):
    """
    For given list of key-value pairs and given base parameters, modify params so
    that k-v pairs are all applied. But leave the actual params object as is and return
    a new object param_variation.
    """
    param_variation = params.copy()
    for k, v in kv_combination:
        param_variation[k] = v
    return param_variation


def _get_cartesian_product(items: list[(str, any)]):
    """
    For dict items with lists of values to each key, return cartesian product of key-value pairs.
    Result will not contain duplicates.
    Example:
        items = [("size", [1000, 2000]), ("overlap", [0.1, 0.2, 0.3])]
        will return
        [((size, 1000), (overlap, 0.1)), ((size, 1000), (overlap, 0.2)), ((size, 1000), (overlap, 0.3)),
        ((size, 2000), (overlap, 0.1)), ((size, 2000), (overlap, 0.2)), ((size, 2000), (overlap, 0.3))]
    """
    all_kv_lists = []
    for k, values in items:
        kv_list = [(k, v) for v in values]  # = [(size, 1000), (size, 2000), ...]
        all_kv_lists.append(kv_list)
    cartesian_prod = itertools.product(*all_kv_lists)  # = [((size, 1000), (overlap, 0.1)), ...]
    return [*cartesian_prod]


def sample_all_down_if_needed(variant_group):
    """
    On each element in the passed list do:

        Get a down-sampled version of the passed dataset variant according to the specified downsampling mode.
        Downsampling mode can be one of:
            None                -> return the original variant
            "TO_MIN_GROUP_SIZE" -> sample down to the size of the smallest subset in the group
            An integer          -> sample down to the specified size

    And return the resulting list.
    Each element in the passed list should be tuple of a dataset variant and its parameter dict, where the downsampling
    mode is specified under the top-level key "downsampling".
    """
    result = []
    min_group_size = min(variant[0].shape[0] for variant in variant_group)  # size of the smallest variant in this group
    for variant, params in tqdm(variant_group, desc="Downsampling",
                                bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}',
                                leave=False):
        # sample down if necessary
        downsampling_mode = params.get("downsampling", None)
        variant = _sample_down_if_needed(min_group_size, downsampling_mode, variant)
        result.append((variant, params))
    return result


def _sample_down_if_needed(min_group_size, downsampling_mode, variant):
    """
    Return a down-sampled version of the passed dataset variant according to the specified downsampling mode.
    Downsampling mode can be one of:
        None                -> return the original variant
        "TO_MIN_GROUP_SIZE" -> sample down to the min_group_size (assumed to be the size of the smallest subset in the
                                group)
        An integer          -> sample down to the specified size
    """
    if downsampling_mode is None:
        return variant
    if downsampling_mode == "TO_MIN_GROUP_SIZE":
        downsampling_size = min_group_size
    elif isinstance(downsampling_mode, int):
        downsampling_size = downsampling_mode
    else:
        raise ValueError(f"Bad downsampling parameter '{downsampling_mode}'.")
    variant = random_sample_wrapper(variant, downsampling_size)
    return variant


def write_group_folder(variant_group, group_id, description, outfile_directory):
    # create this group's sub folder
    group_sub_folder = os.path.join(outfile_directory, f"group_{group_id}")
    Path(group_sub_folder).mkdir(exist_ok=True)
    _create_description_txt(description, group_sub_folder)
    variant_id = 0
    for variant, params in tqdm(variant_group, desc="Variant",
                                bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}',
                                leave=False):
        # create this variant's sub folder
        variant_sub_folder = os.path.join(group_sub_folder, f"DV_{variant_id}")
        Path(variant_sub_folder).mkdir(exist_ok=True)
        # create records.csv, params.json and comment.txt
        variant.to_csv(os.path.join(variant_sub_folder, "records.csv"), index=False, header=False)
        _create_params_json(params, variant, variant_sub_folder)
        variant_id += 1


def _create_description_txt(desc: str, location):
    outpath = os.path.join(location, "desc.txt")
    with open(outpath, 'w') as outfile:
        outfile.write(desc)


def _create_params_json(params, variant, variant_sub_folder):
    actual_size = variant.shape[0]
    # assert that the size stored in params (if it exists) is equal to the size of the variant
    assert params.get("size", actual_size) == actual_size
    # set the size value in params to the actual size of the dataset, in case it wasn't set already
    params["size"] = actual_size
    # create params.json
    write_json(params, os.path.join(variant_sub_folder, "params.json"))


def get_dist_as_dict(params):
    """
    Returns the distribution specified under the "dist" key and transforms string-keys to tuples if "dist_is_range" is
    set to true. Useful because for some distributions (e.g. age) ranges are preferred as keys, but JSON only allows
    strings as keys, while python also allows tuples.
    For example parameter see test_dm_config_attr_val_dist.json.
    """
    if params.get("dist_is_range", False):
        dist = {}
        for k, v in params["dist"].items():
            k_eval = literal_eval(k)  # key evaluated
            if not (isinstance(k_eval, tuple) and len(k_eval) == 2 and k_eval[0] <= k_eval[1]):
                raise ValueError(f"Cannot interpret '{k}' as range-tuple. (must contain two values separated by comma, "
                                 f"the second one must be greater or equal to the first one.")
            dist[k_eval] = v
    else:
        dist = params["dist"]
    return dist


if __name__ == "__main__":
    main()
