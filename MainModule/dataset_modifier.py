import logging
import os.path
import shutil
from pathlib import Path
from typing import Union

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
    def __init__(self, omit_if_not_possible=True, omit_if_too_small=True, min_size_per_source=10):
        """
        :param omit_if_not_possible: Set to True (default) if a variant should be omitted instead of raising a
        ValueError, in case a specified ds-variant cannot be drawn (for example, a random sample with an overlap too large)
        """
        self.omit_if_not_possible = omit_if_not_possible
        self.omit_if_too_small = omit_if_too_small
        self.omitted_too_small = 0
        self.omitted_invald_params = 0
        self.min_size_per_source = min_size_per_source
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
        variant_id = 0
        for param_variant_group, description in tqdm(param_variant_groups, desc="Groups"):
            variant_group = self._get_ds_variants(param_variant_group)
            min_group_size = min(v[0].shape[0] for v in variant_group)  # size of the smallest variant in this group
            # create this group's sub folder
            group_sub_folder = os.path.join(outfile_directory, f"group_{group_id}")
            Path(group_sub_folder).mkdir(exist_ok=True)
            _create_description_txt(description, group_sub_folder)
            for variant, params in tqdm(variant_group, desc="Variant",
                                        bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}',
                                        leave=False):
                # sample down if necessary # TODO
                if params.get("downsampling", None) == "TO_MIN_GROUP_SIZE":
                    variant = random_sample_wrapper(variant, min_group_size)
                # create this variant's sub folder
                variant_sub_folder = os.path.join(group_sub_folder, f"DV_{variant_id}")
                Path(variant_sub_folder).mkdir(exist_ok=True)
                # create records.csv, params.json and comment.txt
                variant.to_csv(os.path.join(variant_sub_folder, "records.csv"), index=False, header=False)
                _create_params_json(params, variant, variant_sub_folder)
                variant_id += 1
            group_id += 1
        self.log_and_reset_omitted()

    def _get_ds_variants(self, param_variants):
        variants = []
        for params in param_variants:
            variant = self.get_variant(params)
            if variant is None:
                # variant could be None if for example a ValueError occurred due to impossible sample size in random
                #  subset
                self.omitted_invald_params += 1
                continue
            if self._check_if_variant_should_be_omitted(variant):
                self.omitted_too_small += 1
                continue
            variants.append((variant, params))
        return variants

    def _check_if_variant_should_be_omitted(self, variant):
        if not self.omit_if_too_small:
            return False
        a_b = [x for _, x in variant.groupby(self.source_id_col_name)]  # split into the two sources
        if len(a_b) < 2:
            # at least one of the two sources has size 0
            observed_min_size = 0
        else:
            observed_min_size = min(a_b[0].shape[0], a_b[1].shape[0])
        if observed_min_size < self.min_size_per_source:
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

    def log_and_reset_omitted(self):
        if self.omitted_too_small:
            logging.info(
                f"Omitted {self.omitted_too_small} variants because they were smaller than min_size_per_source")
        if self.omitted_invald_params:
            logging.info(
                f"Omitted {self.omitted_invald_params} variants because they could not be created (possibly due to impossible parameter "
                f"combination). See logs for further info.")


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
            handle_ranges(replacements)
        cartesian_prod = _get_cartesian_product(replacements.items())
        for kv_combination in cartesian_prod:
            param_variation = _get_param_variation(kv_combination, params)
            param_vars.append(param_variation)
        if variation.get("include_default", False):
            param_vars.append(params)
        groups.append((param_vars, description))
    return groups


# def get_param_variations(config: dict):
#     """
#     Return a list of all parameter variations created from given config dict.
#     If variation lists for multiple parameters are given in one replacement, they
#     are combined to their cartesian product.
#     For each parameter variation, a tuple containing (variant, comment) is returned, comment being the comment in the
#     config file describing this param variation, or an empty string if no comment was specified.
#     """
#     param_vars = []
#     for variation in config["variations"]:
#         comment = variation.get("comment", "")
#         params = variation["params"]
#         replacements = variation.get("replacements", {})
#         if variation.get("as_range", False):
#             handle_ranges(replacements)
#         cartesian_prod = _get_cartesian_product(replacements.items())
#         for kv_combination in cartesian_prod:
#             param_variation = _get_param_variation(kv_combination, params)
#             param_vars.append((param_variation, comment))
#         if variation.get("include_default", False):
#             param_vars.append((params, comment))
#     return param_vars


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


def _create_description_txt(desc: str, location):
    outpath = os.path.join(location, "desc.txt")
    with open(outpath, 'w') as outfile:
        outfile.write(desc)


if __name__ == "__main__":
    main()
