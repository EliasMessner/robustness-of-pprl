from unittest import TestCase, main

from dataset_modifier import DatasetModifier, get_param_variations
import pandas as pd
from datetime import datetime as dt

filepath = "data/2021_NCVR_Panse_001/dataset_ncvr_dirty.csv"
col_names = "sourceID,globalID,localID,FIRSTNAME,MIDDLENAME,LASTNAME,YEAROFBIRTH,PLACEOFBIRTH,COUNTRY,CITY,PLZ,STREET,GENDER,ETHNIC,RACE".split(
    ",")


class TestDatasetModifier(TestCase):
    def setUp(self) -> None:
        self.sg = DatasetModifier()
        self.sg.load_dataset_by_config_file("data/test_dataset_modifier.json")

    def test_size(self):
        self.assertEqual(self.sg.df.shape[0], 200000)
        self.assertEqual(self.sg.df1.shape[0], 100000)
        self.assertEqual(self.sg.df2.shape[0], 100000)
        self.assertTrue(self.sg.df1.sourceID.eq("A").all(axis=0))
        self.assertTrue(self.sg.df2.sourceID.eq("B").all(axis=0))

    def test_random_sample(self):
        # draw sample of size 10 with original overlap
        sample = self.sg.random_sample({"size": 10, "seed": 1})
        sample_a = sample[sample[self.sg.source_id_col_name].isin(
            self.sg.df1[self.sg.source_id_col_name])]  # records in sample from source A
        sample_b = sample[sample[self.sg.source_id_col_name].isin(
            self.sg.df2[self.sg.source_id_col_name])]  # records in sample from source B
        # check that the overlap is correct
        expected_overlap = self.sg.base_overlap
        intersec = pd.merge(sample_a, sample_b, how="inner", on=self.sg.global_id_col_name)
        observed_overlap = 2 * intersec.shape[0] / sample.shape[0]
        self.assertEqual(expected_overlap, observed_overlap)

        # sample of size 2*387 with original overlap
        sample = self.sg.random_sample({"size": 2*387, "seed": 1})
        sample_a = sample[sample[self.sg.source_id_col_name].isin(
            self.sg.df1[self.sg.source_id_col_name])]  # records in sample from source A
        sample_b = sample[sample[self.sg.source_id_col_name].isin(
            self.sg.df2[self.sg.source_id_col_name])]  # records in sample from source B
        # check that the overlap is correct
        intersec = pd.merge(sample_a, sample_b, how="inner", on=self.sg.global_id_col_name)
        expected_size = round(expected_overlap * sample.shape[0] / 2)  # round because only whole records are counted
        observed_size = intersec.shape[0]
        self.assertEqual(expected_size, observed_size)

        # sample size 2*445 with overlap = 0.35
        expected_overlap = 0.35
        sample = self.sg.random_sample({"size": 2*445, "seed": 1, "overlap": expected_overlap})
        sample_a = sample[sample[self.sg.source_id_col_name].isin(
            self.sg.df1[self.sg.source_id_col_name])]  # records in sample from source A
        sample_b = sample[sample[self.sg.source_id_col_name].isin(
            self.sg.df2[self.sg.source_id_col_name])]  # records in sample from source B
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
                    }
                }
            ]
        }
        variations = get_param_variations(config)
        expectation = [
            {"subset_selection": "RANDOM", "seed": 1, "size": 1000, "overlap": 0.1},
            {"subset_selection": "RANDOM", "seed": 1, "size": 2000, "overlap": 0.1},
            {"subset_selection": "RANDOM", "seed": 1, "size": 10000, "overlap": 0.1}
        ]
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
        variations = get_param_variations(config)
        expectation = [
            {"subset_selection": "RANDOM", "seed": 1, "size": 1000, "overlap": 0.1},
            {"subset_selection": "RANDOM", "seed": 1, "size": 2000, "overlap": 0.1},
            {"subset_selection": "RANDOM", "seed": 1, "size": 10000, "overlap": 0.1},
            {"subset_selection": "RANDOM", "seed": 1, "size": 5000, "overlap": 0.1}
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
                 }
            ]
        }
        variations = get_param_variations(config)
        expectation = [
            {"subset_selection": "RANDOM", "seed": 1, "size": 1000, "overlap": 0.1},
            {"subset_selection": "RANDOM", "seed": 1, "size": 1000, "overlap": 0.2},
            {"subset_selection": "RANDOM", "seed": 1, "size": 1000, "overlap": 0.3},
            {"subset_selection": "RANDOM", "seed": 1, "size": 2000, "overlap": 0.1},
            {"subset_selection": "RANDOM", "seed": 1, "size": 2000, "overlap": 0.2},
            {"subset_selection": "RANDOM", "seed": 1, "size": 2000, "overlap": 0.3},
            {"subset_selection": "RANDOM", "seed": 1, "size": 10000, "overlap": 0.1},
            {"subset_selection": "RANDOM", "seed": 1, "size": 10000, "overlap": 0.2},
            {"subset_selection": "RANDOM", "seed": 1, "size": 10000, "overlap": 0.3}
        ]
        self.assertCountEqual(variations, expectation)

    def test_attribute_value_subset(self):
        all_genders = self.sg.attribute_value_subset(params={"column": "GENDER", "is_in": ["M", "F", "U"]})
        pd.testing.assert_frame_equal(all_genders, self.sg.df, check_like=True)
        f = self.sg.attribute_value_subset(params={"column": "GENDER", "equals": "F"})
        self.assertTrue(f.eq("F").all(axis=0)["GENDER"])


if __name__ == '__main__':
    main()
