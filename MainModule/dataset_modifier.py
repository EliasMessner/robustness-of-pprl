import logging
import os.path
import shutil
from pathlib import Path

import pandas as pd
from tqdm import tqdm
import itertools
from datetime import datetime as dt

from dataset_properties import get_overlap, get_true_matches, split_by_source_id
from util import read_json, write_json, get_config_path_from_argv
from constants import dm_config_path, dataset_variants_dir, logs_dir


def main():
    _dm_config_path = get_config_path_from_argv(default=dm_config_path)
    # create dataset variations
    dm = DatasetModifier()
    dm.load_dataset_by_config_file(_dm_config_path)
    shutil.rmtree(dataset_variants_dir, ignore_errors=True)  # delete existing dataset variants
    dm.create_variants_by_config_file(_dm_config_path, dataset_variants_dir)


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
    def __init__(self, omit_if_not_possible=True):
        """
        :param omit_if_not_possible: Set to True (default) if a variant should be omitted instead of raising a
        ValueError, in case a specified ds-variant cannot be drawn (for example, a random sample with an overlap too large)
        """
        self.omit_if_not_possible = omit_if_not_possible
        self.df = None
        self.df_a = None
        self.df_b = None
        self.source_id_col_name = None
        self.global_id_col_name = None
        self.true_matches1 = None
        self.true_matches2 = None
        self.base_overlap = None

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
        self.true_matches1, self.true_matches2 = get_true_matches(self.df_a, self.df_b, self.global_id_col_name)
        self.base_overlap = get_overlap(self.df_a, self.df_b, self.global_id_col_name)

    def create_variants_by_config_file(self, config_path, outfile_directory, omit_if_too_small=True,
                                       min_size_per_source=10):
        """
        Read dataset modifier config file, create dataset variations as described in the file, write them all to
        out_location folder. Each variant goes in a sub folder containing its parameters in params.json and its records
        in records.csv.
        :param omit_if_too_small: if set to True (default), only store variant if each source contains at least
            min_size_per_source records.
        :param min_size_per_source: minimum number of records required in each source to keep the dataset. Will be
            ignored if omit_if_too_small is set to False. Defaults to 10.
        """
        config = read_json(config_path)
        variants = self.get_variants_by_config_dict(config)
        Path(outfile_directory).mkdir(exist_ok=True)
        variant_id = 0
        omitted = 0
        for (params, variant) in tqdm(variants, desc="Saving Variants", bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}'):
            if self._check_if_variant_should_be_omitted(min_size_per_source, omit_if_too_small, variant):
                omitted += 1
                continue
            # create this variant's sub folder
            variant_sub_folder = os.path.join(outfile_directory, f"DV_{variant_id}")
            Path(variant_sub_folder).mkdir(exist_ok=True)
            # create records.csv and params.json
            variant.to_csv(os.path.join(variant_sub_folder, "records.csv"), index=False, header=False)
            _create_params_json(params, variant, variant_sub_folder)
            variant_id += 1
        if omitted:
            logging.info(f"Omitted {omitted} variants because they were smaller than {min_size_per_source}")

    def get_variants_by_config_dict(self, config) -> list[(dict, pd.DataFrame)]:
        """
        Returns list of tuples: (params, variant), with params a dict of parameter key-value pairs, and variant the
        variant created by those params as DataFrame.
        """
        self.read_csv_config_dict(config)  # read the dataset
        variants = []
        omitted = 0
        for params in tqdm(get_param_variations(config), desc="Creating Variants",
                           bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}'):
            variant = self.get_variant(params)
            if variant is None:
                # variant could be None if for example a ValueError occurred due to impossible sample size in random
                #  subset
                omitted += 1
                continue
            variants.append((params, variant))
        if omitted:
            logging.info(
                f"Omitted {omitted} variants because they could not be created (possibly due to impossible parameter "
                f"combination). See logs for further info.")
        return variants

    def _check_if_variant_should_be_omitted(self, min_size_per_source, omit_if_too_small, variant):
        if not omit_if_too_small:
            return False
        a_b = [x for _, x in variant.groupby(self.source_id_col_name)]  # split into the two sources
        if len(a_b) < 2:
            # at least one of the two sources has size 0
            observed_min_size = 0
        else:
            observed_min_size = min(a_b[0].shape[0], a_b[1].shape[0])
        if observed_min_size < min_size_per_source:
            return True
        return False

    def get_variant(self, params) -> pd.DataFrame:
        if params["subset_selection"] == "RANDOM":
            return self.random_sample(params)
        if params["subset_selection"] == "ATTRIBUTE_VALUE":
            return self.attribute_value_subset(params)
        if params["subset_selection"] == "PLZ":
            return self.plz_subset(params)
        if params["subset_selection"] == "AGE":
            return self.age_subset(params)

    def random_sample(self, params) -> pd.DataFrame | None:
        """
        Draw random sample from base dataset.
        If the desired overlap cannot be drawn, ...
            ...returns None, if self.omit_if_not_possible == True
            ...raises a ValueError, otherwise
        :param params: dict containing the keys described below.
        size (int): number of records to draw all together (the ratio between source A and B will be preserved as
        closely as possible after rounding)
        seed (int): Seed for reproducibility
        overlap (float) (optional): ratio of true matches to whole size of one source, if not specified the ratio will
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
        assert len({"range", "equals", "is_in"} & params.keys()) == 1  # exactly one of these keys must be present
        if "equals" in params:
            return self.df[self.df[params["column"]] == params["equals"]]
        if "is_in" in params:
            return self.df[self.df[params["column"]].map(lambda value: value in params["is_in"])]
        if "range" in params:
            min_v = params["range"][0]
            max_v = params["range"][1]
            return self.df[self.df[params["column"]].map(lambda value: min_v <= value <= max_v)]

    def plz_subset(self, params) -> pd.DataFrame:
        n = params["digits"]  # first n digits
        value = params["equals"]  # must be equal to value
        return self.df[self.df["PLZ"].map(lambda plz: plz.isdigit() and int(plz[0:n]) == value)]

    def age_subset(self, params) -> pd.DataFrame:
        this_year = dt.now().year
        min_age = params["range"][0]
        max_age = params["range"][1]
        return self.df[self.df["YEAROFBIRTH"].map(lambda yob: min_age <= (this_year - yob) <= max_age)]

    def _calculate_base_overlap(self):
        """
        Returns overlap in base dataset. Overlap is calculated wrt. size of one source,
        which means that if there are two datasets A and B with each 100 records,
        20 of which are matches and 80 non-matches, then the overlap will be 0.2.
        :return: Overlap value between 0 and 1.
        :rtype: float
        """
        intersect = pd.merge(self.df_a, self.df_b, how="inner", on=[self.global_id_col_name])
        self.base_overlap = 2 * intersect.shape[0] / self.df.shape[0]


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
    a_non_matches = df_a[~df_a[global_id_col_name].isin(true_matches_a[global_id_col_name])]\
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


def get_param_variations(config: dict):
    """
    Return a list of all parameter variations created from given config dict.
    If variation lists for multiple parameters are given in one replacement, they
    are combined to their cartesian product.
    """
    param_vars = []
    for variation in config["variations"]:
        params = variation["params"]
        replacements = variation.get("replacements", {})
        if variation.get("as_range", False):
            handle_ranges(replacements)
        cartesian_prod = _get_cartesian_product(replacements.items())
        for kv_combination in cartesian_prod:
            param_variation = _get_param_variation(kv_combination, params)
            param_vars.append(param_variation)
        if variation.get("include_default", False):
            param_vars.append(params)
    return param_vars


def handle_ranges(replacements):
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


def _create_params_json(params, variant, variant_sub_folder):
    actual_size = variant.shape[0]
    # assert that the size stored in params (if it exists) is equal to the size of the variant
    assert params.get("size", actual_size) == actual_size
    # set the size value in params to the actual size of the dataset, in case it wasn't set already
    params["size"] = actual_size
    # create params.json
    write_json(params, os.path.join(variant_sub_folder, "params.json"))


if __name__ == "__main__":
    main()
