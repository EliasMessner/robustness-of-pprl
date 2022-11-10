from subset_generator import SubsetGenerator
import unittest
import pandas as pd


filepath = "data/2021_NCVR_Panse_001/dataset_ncvr_dirty.csv"
col_names = "sourceID,globalID,localID,FIRSTNAME,MIDDLENAME,LASTNAME,YEAROFBIRTH,PLACEOFBIRTH,COUNTRY,CITY,PLZ,STREET,GENDER,ETHNIC,RACE".split(",")


class Test_SubsetGenerator(unittest.TestCase):
    def setUp(self) -> None:
        self.sg = SubsetGenerator(filepath, col_names)
    
    def test_size(self):
        self.assertEqual(self.sg.df.shape[0], 200000)
        self.assertEqual(self.sg.df1.shape[0], 100000)
        self.assertEqual(self.sg.df2.shape[0], 100000)
        self.assertTrue(self.sg.df1.sourceID.eq("A").all(axis=0))
        self.assertTrue(self.sg.df2.sourceID.eq("B").all(axis=0))

    def test_random_sample(self):
        # draw sample of size 10 with original overlap
        sample = self.sg.random_sample(size=10, seed=1)
        sample_a = sample[sample[self.sg.source_id_col_name].isin(self.sg.df1[self.sg.source_id_col_name])]  # records in sample from source A
        sample_b = sample[sample[self.sg.source_id_col_name].isin(self.sg.df2[self.sg.source_id_col_name])]  # records in sample from source B
        # check that the overlap is correct
        expected_overlap = self.sg._base_overlap
        intersec = pd.merge(sample_a, sample_b, how="inner", on=self.sg.global_id_col_name)
        observed_overlap = 2 * intersec.shape[0] / sample.shape[0]
        self.assertEqual(expected_overlap, observed_overlap)

        # sample of size 387 with original overlap
        sample = self.sg.random_sample(size=387, seed=1)
        sample_a = sample[sample[self.sg.source_id_col_name].isin(self.sg.df1[self.sg.source_id_col_name])]  # records in sample from source A
        sample_b = sample[sample[self.sg.source_id_col_name].isin(self.sg.df2[self.sg.source_id_col_name])]  # records in sample from source B
        # check that the overlap is correct
        intersec = pd.merge(sample_a, sample_b, how="inner", on=self.sg.global_id_col_name)
        expected_size = round(expected_overlap * sample.shape[0] / 2)  # round because only whole records are counted
        observed_size = intersec.shape[0]
        self.assertEqual(expected_size, observed_size)
    
        # sample size 445 with overlap = 0.35
        expected_overlap = 0.35
        sample = self.sg.random_sample(size=445, seed=1, overlap=expected_overlap)
        sample_a = sample[sample[self.sg.source_id_col_name].isin(self.sg.df1[self.sg.source_id_col_name])]  # records in sample from source A
        sample_b = sample[sample[self.sg.source_id_col_name].isin(self.sg.df2[self.sg.source_id_col_name])]  # records in sample from source B
        # check that the overlap is correct
        intersec = pd.merge(sample_a, sample_b, how="inner", on=self.sg.global_id_col_name)
        expected_size = round(expected_overlap * sample.shape[0] / 2)  # round because only whole records are counted
        observed_size = intersec.shape[0]
        self.assertEqual(expected_size, observed_size)

        # sample size 100k with overlap = 0.35
        # must raise value error because original overlap is only 0.2 and dataset size is 200k
        expected_overlap = 0.35
        self.assertRaises(ValueError, self.sg.random_sample, size=100000, seed=1, overlap=expected_overlap)

        # sample size 150k with overlap = 0.1
        # must raise value error because only 100k values are in each source
        expected_overlap = 0.1
        self.assertRaises(ValueError, self.sg.random_sample, size=150000, seed=1, overlap=expected_overlap)

    # TODO test with specified overlap
    # TODO test with odd values

if __name__ == '__main__':
    unittest.main()