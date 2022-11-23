import os.path
from pathlib import Path

import pandas as pd
from tqdm import tqdm
import itertools

from util import read_json, write_json
from constants import dm_config_path, dataset_variants_dir


def main():
    # create dataset variations
    dm = DatasetModifier()
    dm.load_dataset_by_config_file(dm_config_path)
    dm.create_variants_by_config_file(dm_config_path, dataset_variants_dir)


def get_param_variations(config):
    """
    Return a list of all parameter variations created from given config dict.
    If variation lists for multiple parameters are given in one replacement, they
    are combined to their cartesian product.
    """
    # param_vars = []
    # for params in config["variations"]:
    #     for k, values in params["replacements"].items():
    #         for v in values:
    #             param_variation = params.copy()
    #             param_variation.pop('replacements', None)
    #             param_variation[k] = v
    #             param_vars.append(param_variation)
    # return param_vars
    param_vars = []
    for params in config["variations"]:
        cartesian_prod = _get_cartesian_product(params["replacements"].items())
        for kv_combination in cartesian_prod:
            param_variation = _get_param_variation(kv_combination, params)
            param_vars.append(param_variation)
    return param_vars


def _get_param_variation(kv_combination, params):
    """
    For given list of key-value pairs and given base parameters, modify params so
    that k-v pairs are all applied. But leave the actual params object as is and return
    a new object param_variation.
    """
    param_variation = params.copy()
    param_variation.pop('replacements', None)
    for k, v in kv_combination:
        param_variation[k] = v
    return param_variation


def _get_cartesian_product(items):
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
    return list({*cartesian_prod})


class DatasetModifier:
    def __init__(self):
        self.df = None
        self.df1 = None
        self.df2 = None
        self.source_id_col_name = None
        self.global_id_col_name = None
        self.true_matches1 = None
        self.true_matches2 = None
        self._base_overlap = None

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
        self.df1, self.df2 = [x for _, x in self.df.groupby(source_id_col_name)]
        assert self.df1.shape == self.df2.shape
        self.global_id_col_name = global_id_col_name
        self.source_id_col_name = source_id_col_name
        self.true_matches1, self.true_matches2 = self._get_true_matches()
        self._base_overlap = self._get_base_overlap()

    def create_variants_by_config_file(self, config_path, outfile_directory):
        """
        Read dataset modifier config file, create dataset variations as described in the file, write them all to
        out_location folder. Each variant goes in a sub folder containing its parameters in params.json and its records
        in records.csv.
        """
        config = read_json(config_path)
        variants = self.get_variants_by_config_dict(config)
        Path(outfile_directory).mkdir(exist_ok=True)
        variant_id = 0
        for (params, variant) in tqdm(variants, desc="Saving Variants", bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}'):
            # create this variant's sub folder
            variant_sub_folder = os.path.join(outfile_directory, f"DV_{variant_id}")
            Path(variant_sub_folder).mkdir(exist_ok=True)
            # create records.csv
            variant.to_csv(os.path.join(variant_sub_folder, "records.csv"), index=False, header=False)
            # create param.json
            write_json(params, os.path.join(variant_sub_folder, "params.json"))
            variant_id += 1

    def get_variants_by_config_dict(self, config) -> list[(dict, pd.DataFrame)]:
        """
        Returns list of tuples: (params, variant), with params a dict of parameter key-value pairs, and variant the
        variant created by those params as DataFrame.
        """
        self.read_csv_config_dict(config)  # read the dataset
        return [(params, self.get_variant(params)) for params in get_param_variations(config)]

    def get_variant(self, params):
        if params["subset_selection"] == "RANDOM":
            return self.random_sample(params)
        # TODO add missing subset selectors

    def random_sample(self, params):
        """
        Draw random sample from base dataset.
        :param params: dict containing the keys described below.
        size (int): number of records to draw from each of the two sources
        seed (int): Seed for reproducibility
        overlap (float) (optional): ratio of true matches to whole size of one source, if not specified the ratio will
        be the same
        as in the base dataset
        :return: random sample drawn from base dataframe
        """
        try:
            return self._random_sample(size=params["size"], seed=params["seed"], overlap=params.get("overlap", None))
        except ValueError as e:
            raise ValueError(f"{e}\nParameters causing ValueError: {params}")

    def _random_sample(self, size: int, seed: int, overlap: float = None) -> pd.DataFrame:
        if not (0 <= size <= self.df1.shape[0]):
            raise ValueError(
                f"Size must be between 0 and size of one of the two source data sets (={self.df1.shape[0]}). Got "
                f"{size} instead.")
        rel_sample_size = size / self.df.shape[0]
        max_overlap = min(1.0, self._base_overlap / rel_sample_size)
        if overlap is None:
            overlap = self._base_overlap
        if not (0 <= overlap <= max_overlap or overlap is None):
            raise ValueError(f"Overlap must be between 0 and {max_overlap}. Got {overlap} instead.")
        # draw size*overlap from true matches of source A
        a_matches = self.true_matches1.sample(round(size * overlap), random_state=seed)
        # draw size*(1-overlap) from non-matches of source A
        a_non_matches = self.df1[~self.df1[self.global_id_col_name].isin(self.true_matches1[self.global_id_col_name])] \
            .sample(round(size * (1 - overlap)), random_state=seed)
        # draw size*overlap from true matches of source B
        b_matches = self.true_matches2.sample(round(size * overlap), random_state=seed)
        # draw size*(1-overlap) from non-matches of source B
        b_non_matches = self.df2[~self.df2[self.global_id_col_name].isin(self.true_matches2[self.global_id_col_name])] \
            .sample(round(size * (1 - overlap)), random_state=seed)
        # concatenate all
        return pd.concat([a_matches, a_non_matches, b_matches, b_non_matches])

    def get_subsets_by_param_group_by(self, param, mapping_to_group_by: callable) -> dict:
        """
        Return the subsets that result from grouping by a new column
        """
        values_to_group_by = self.df[param].map(mapping_to_group_by)
        groups = self.df.groupby(values_to_group_by)
        return {key: groups.get_group(key) for key in set(groups.keys)}

    def get_subset_by_parameter_match(self, param: str, values: list[str], complement=False, match_case=True):
        if match_case:
            condition = self.df[param].isin(values)
        else:
            condition = self.df[param].str.lower().isin([v.lower() for v in values])
        if complement:
            condition = ~condition
        return self.df[condition]

    def _get_true_matches(self) -> (pd.DataFrame, pd.DataFrame):
        """
        Return form each source all records that are part of the overlap, aka all records that have a true match in the
        other source.
        Returned as tuple of two dataframes, one for each sourceID.
        They are not linked to their partners, i.e. the dataframes have no specific order.
        """
        true_matches_1 = self.df1[self.df1[self.global_id_col_name].isin(self.df2[self.global_id_col_name])]
        true_matches_2 = self.df2[self.df2[self.global_id_col_name].isin(self.df1[self.global_id_col_name])]
        return true_matches_1, true_matches_2

    def _get_base_overlap(self):
        """
        Returns overlap in base dataset. Overlap is calculated wrt. size of one source,
        which means that if there are two datasets A and B with each 100 records,
        20 of which are matches and 80 non-matches, then the overlap will be 0.2.
        :return: Overlap value between 0 and 1.
        :rtype: float
        """
        intersect = pd.merge(self.df1, self.df2, how="inner", on=[self.global_id_col_name])
        return 2 * intersect.shape[0] / self.df.shape[0]


if __name__ == "__main__":
    main()
