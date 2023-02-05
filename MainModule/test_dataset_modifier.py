import os.path
import shutil
from datetime import datetime as dt
from unittest import TestCase, main

import pandas as pd

from constants import dataset_variants_dir_test
from dataset_modifier import DatasetModifier, get_param_variant_groups, random_sample
from dataset_properties import get_overlap, split_by_source_id, split_and_get_overlap
from error_rates import get_all_errors
from util import read_json, list_folder_names

filepath = "data/2021_NCVR_Panse_001/dataset_ncvr_dirty.csv"
col_names = "sourceID,globalID,localID,FIRSTNAME,MIDDLENAME,LASTNAME,YEAROFBIRTH,PLACEOFBIRTH,COUNTRY,CITY,PLZ," \
            "STREET,GENDER,ETHNIC,RACE".split(",")


class TestDatasetModifier(TestCase):
    def setUp(self) -> None:
        self.sg = DatasetModifier(omit_if_not_possible=False)
        self.sg.load_dataset_by_config_file("data/test_dataset_modifier.json")

    def test_size(self):
        self.assertEqual(self.sg.df.shape[0], 200000)
        self.assertEqual(self.sg.df_a.shape[0], 100000)
        self.assertEqual(self.sg.df_b.shape[0], 100000)
        self.assertTrue(self.sg.df_a.sourceID.eq("A").all(axis=0))
        self.assertTrue(self.sg.df_b.sourceID.eq("B").all(axis=0))

    def test_random_sample(self):
        # draw sample of size 10 with original overlap
        sample = self.sg.random_sample({"size": 10, "seed": 1})
        sample_a = sample[sample[self.sg.source_id_col_name].isin(
            self.sg.df_a[self.sg.source_id_col_name])]  # records in sample from source A
        sample_b = sample[sample[self.sg.source_id_col_name].isin(
            self.sg.df_b[self.sg.source_id_col_name])]  # records in sample from source B
        # check that the overlap is correct
        expected_overlap = self.sg.base_overlap
        observed_overlap = get_overlap(sample_a, sample_b)
        self.assertEqual(expected_overlap, observed_overlap)

        # sample of size 2*387 with original overlap
        sample = self.sg.random_sample({"size": 2 * 387, "seed": 1})
        sample_a, sample_b = split_by_source_id(sample)
        # check that the overlap is correct
        intersec = pd.merge(sample_a, sample_b, how="inner", on=self.sg.global_id_col_name)
        expected_size = round(expected_overlap * sample.shape[0] / 2)  # round because only whole records are counted
        observed_size = intersec.shape[0]
        self.assertEqual(expected_size, observed_size)

        # sample size 2*445 with overlap = 0.35
        expected_overlap = 0.35
        sample = self.sg.random_sample({"size": 2 * 445, "seed": 1, "overlap": expected_overlap})
        sample_a, sample_b = split_by_source_id(sample)
        # check that the overlap is correct
        intersec = pd.merge(sample_a, sample_b, how="inner", on=self.sg.global_id_col_name)
        expected_size = round(expected_overlap * sample.shape[0] / 2)  # round because only whole records are counted
        observed_size = intersec.shape[0]
        self.assertEqual(expected_size, observed_size)

        # sample size 200k with overlap = 0.35
        # must raise value error because original overlap is only 0.2 and dataset size is 200k
        expected_overlap = 0.35
        self.assertRaises(ValueError, self.sg.random_sample, {"size": 200000, "seed": 1, "overlap": expected_overlap})

        # sample size 300k with overlap = 0.1
        # must raise value error because only 100k values are in each source
        expected_overlap = 0.1
        self.assertRaises(ValueError, self.sg.random_sample, {"size": 300000, "seed": 1, "overlap": expected_overlap})

        # test with differently sized sources
        expected_overlap = 0.1
        df_a, df_b = split_by_source_id(self.sg.df)
        df_a = df_a.sample(80_000, random_state=42)
        self.assertEqual(df_a.shape[0], 80_000)
        self.assertEqual(df_b.shape[0], 100_000)
        sample = random_sample(df_a, df_b, 75_000, seed=42, overlap=expected_overlap)
        sample_a, sample_b = split_by_source_id(sample)
        # check that source a/b ratio is preserved
        self.assertEqual(round(sample_a.shape[0] / sample_b.shape[0], 3),
                         round(df_a.shape[0] / df_b.shape[0], 3))
        # check that the overlap is correct
        self.assertEqual(expected_overlap, get_overlap(sample_a, sample_b))

    def test_plz_subset(self):
        subset0 = self.sg.plz_subset({"digits": 1, "equals": 0})
        subset2 = self.sg.plz_subset({"digits": 1, "equals": 2})
        self.assertTrue(subset0["PLZ"].map(lambda plz: int(plz[:1]) == 0).all())
        self.assertTrue(subset2["PLZ"].map(lambda plz: int(plz[:1]) == 2).all())

    def test_age_subset(self):
        this_year = dt.now().year
        subset_12_22 = self.sg.age_subset({"range": [12, 22]})
        age = subset_12_22["YEAROFBIRTH"].map(lambda yob: this_year - yob)
        self.assertTrue(age.map(lambda a: 12 <= a <= 22).all())

    def test_get_param_variations(self):
        config = {
            "variations": [
                {
                    "params": {
                        "subset_selection": "RANDOM",
                        "seed": 1,
                        "size": 5000,
                        "overlap": 0.1
                    },
                    "replacements": {
                        "size": [1000, 2000, 10000],
                    },
                    "desc": "test description"
                }
            ]
        }
        expectation = [
            ([
                 {"subset_selection": "RANDOM", "seed": 1, "size": 1000, "overlap": 0.1},
                 {"subset_selection": "RANDOM", "seed": 1, "size": 2000, "overlap": 0.1},
                 {"subset_selection": "RANDOM", "seed": 1, "size": 10000, "overlap": 0.1}
             ],
             "test description")
        ]

        variations = get_param_variant_groups(config)
        self.assertCountEqual(variations, expectation)

        # same but with include_default = true
        config = {
            "variations": [
                {
                    "params": {
                        "subset_selection": "RANDOM",
                        "seed": 1,
                        "size": 5000,
                        "overlap": 0.1
                    },
                    "replacements": {
                        "size": [1000, 2000, 10000],
                    },
                    "include_default": True
                }
            ]
        }
        variations = [group for group, desc in get_param_variant_groups(config)]
        expectation = [
            [
                {"subset_selection": "RANDOM", "seed": 1, "size": 1000, "overlap": 0.1},
                {"subset_selection": "RANDOM", "seed": 1, "size": 2000, "overlap": 0.1},
                {"subset_selection": "RANDOM", "seed": 1, "size": 10000, "overlap": 0.1},
                {"subset_selection": "RANDOM", "seed": 1, "size": 5000, "overlap": 0.1}
            ]
        ]
        self.assertCountEqual(variations, expectation)

        config = {
            "variations": [
                {
                    "params": {
                        "subset_selection": "RANDOM",
                        "seed": 1,
                        "size": 20000,
                        "overlap": 0.1
                    },
                    "replacements": {
                        "size": [1000, 2000, 10000],
                        "overlap": [0.1, 0.2, 0.3]
                    }
                },
                {
                    "params": {
                        "subset_selection": "AGE",
                        "range": [0, 19]
                    },
                    "replacements": {
                        "range": [[20, 39]]
                    },
                    "include_default": True
                }
            ]
        }
        variations = [group for group, desc in get_param_variant_groups(config)]
        expectation = [
            [
                {"subset_selection": "RANDOM", "seed": 1, "size": 1000, "overlap": 0.1},
                {"subset_selection": "RANDOM", "seed": 1, "size": 1000, "overlap": 0.2},
                {"subset_selection": "RANDOM", "seed": 1, "size": 1000, "overlap": 0.3},
                {"subset_selection": "RANDOM", "seed": 1, "size": 2000, "overlap": 0.1},
                {"subset_selection": "RANDOM", "seed": 1, "size": 2000, "overlap": 0.2},
                {"subset_selection": "RANDOM", "seed": 1, "size": 2000, "overlap": 0.3},
                {"subset_selection": "RANDOM", "seed": 1, "size": 10000, "overlap": 0.1},
                {"subset_selection": "RANDOM", "seed": 1, "size": 10000, "overlap": 0.2},
                {"subset_selection": "RANDOM", "seed": 1, "size": 10000, "overlap": 0.3}
            ],
            [
                {"subset_selection": "AGE", "range": [20, 39]},
                {"subset_selection": "AGE", "range": [0, 19]}
            ]
        ]
        self.assertCountEqual(variations, expectation)

        config = {
            "variations": [
                {
                    "params": {
                        "subset_selection": "PLZ",
                        "digits": 2,
                        "equals": None
                    },
                    "replacements": {
                        "equals": [0, 99]
                    },
                    "as_range": True
                }
            ]
        }
        variations = [group for group, desc in get_param_variant_groups(config)]
        expectation = [
            [
                {"subset_selection": "PLZ", "digits": 2, "equals": e}
                for e in range(0, 99)
            ]
        ]
        self.assertCountEqual(variations, expectation)

    def test_attribute_value_subset(self):
        all_genders = self.sg.attribute_value_subset(params={"column": "GENDER", "is_in": ["M", "F", "U"]})
        pd.testing.assert_frame_equal(all_genders, self.sg.df, check_like=True)
        f = self.sg.attribute_value_subset(params={"column": "GENDER", "equals": "F"})
        self.assertTrue(f.eq("F").all(axis=0)["GENDER"])

    def test_downsampling(self):
        self.sg.load_dataset_by_config_file("data/test_dataset_modifier_downsampling.json")
        shutil.rmtree(dataset_variants_dir_test, ignore_errors=True)  # delete existing dataset variants
        self.sg.create_variants_by_config_file("data/test_dataset_modifier_downsampling.json",
                                               dataset_variants_dir_test)
        self.assertEqual(
            read_json(os.path.join(dataset_variants_dir_test, "group_0/DV_1/params.json")),
            {
                "subset_selection": "ATTRIBUTE_VALUE",
                "column": "GENDER",
                "equals": "F",
                "downsampling": "TO_MIN_GROUP_SIZE",
                "size": 86605
            }
        )
        self.assertEqual(
            read_json(os.path.join(dataset_variants_dir_test, "group_1/DV_1/params.json")),
            {
                "subset_selection": "ATTRIBUTE_VALUE",
                "column": "GENDER",
                "equals": "F",
                "size": 106860
            }
        )

    def check_all_variants_ok(self, dm_config_path: str, check_operation: callable):
        """
        callable must be a function or lambda taking (variant: pd.DataFrame, params: dict) as input and returning a
        bool stating if the given variant is expected wrt. to the given params.
        """
        self.sg.load_dataset_by_config_file(dm_config_path)
        shutil.rmtree(dataset_variants_dir_test, ignore_errors=True)  # delete existing dataset variants
        self.sg.create_variants_by_config_file(dm_config_path, dataset_variants_dir_test)
        param_groups = get_param_variant_groups(read_json(dm_config_path))
        variant_groups_folders = list_folder_names(dataset_variants_dir_test)
        self.assertEqual(len(param_groups), len(variant_groups_folders))
        for (param_group, desc), variant_group_folder in zip(param_groups, variant_groups_folders):
            variant_group = list_folder_names(os.path.join(dataset_variants_dir_test, variant_group_folder))
            param_group = [params for params in param_group
                           if not self.sg._check_if_variant_should_be_omitted(self.sg.get_variant(params))]
            self.assertEqual(len(param_group), len(variant_group))
            for params, variant_folder in zip(param_group, variant_group):
                stored_params = read_json(os.path.join(dataset_variants_dir_test, variant_group_folder,
                                                       variant_folder, "params.json"))
                for k, v in params.items():
                    self.assertEqual(v, stored_params[k])
                variant = pd.read_csv(os.path.join(dataset_variants_dir_test, variant_group_folder, variant_folder,
                                                   "records.csv"),
                                      names=col_names, dtype={"PLZ": str}, keep_default_na=False)
                check_operation(variant, params)

    def error_rates_ok(self, df, params):
        min_e, max_e = params["range"]
        measure = params["measure"]
        errors = get_all_errors(df, measure)
        self.assertTrue(all([min_e <= e <= max_e for e in errors]))

    def test_error_rate_selection(self):
        self.check_all_variants_ok("data/test_dataset_modifier_error_rate.json", self.error_rates_ok)

    def attr_val_dist_ok(self, df, params):
        # check overlap
        if params["preserve_overlap"]:
            self.assertEqual(split_and_get_overlap(df), self.sg.base_overlap)
        # TODO check distribution

    def test_attr_val_dist(self):
        self.check_all_variants_ok("data/test_dataset_modifier_attr_val_dist.json", self.attr_val_dist_ok)


if __name__ == '__main__':
    main()
