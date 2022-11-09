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
        sample = self.sg.random_sample(size=100, seed=1)
        sample_a = sample[sample.sourceID == "A"]
        sample_b = sample[sample.sourceID == "B"]
        # sample_a = pd.merge(self.sg.df1, sample, how="inner", on=self.sg.global_id_col_name)  # should be 100 but is 120, need to find out why. This expression would be better since not dataset dependent
        # check that the overlap is correct
        expected_overlap = self.sg._base_overlap
        intersec = pd.merge(sample_a, sample_b, how="inner", on=self.sg.global_id_col_name)
        observed_overlap = 2 * intersec.shape[0] / sample.shape[0]
        self.assertEqual(expected_overlap, observed_overlap)
    
    # TODO test with specified overlap
    # TODO test with odd values

if __name__ == '__main__':
    unittest.main()