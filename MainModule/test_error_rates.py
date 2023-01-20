from math import ceil
from unittest import TestCase, main

from error_rates import *

filepath = "data/2021_NCVR_Panse_001/dataset_ncvr_dirty.csv"
col_names = "sourceID,globalID,localID,FIRSTNAME,MIDDLENAME,LASTNAME,YEAROFBIRTH,PLACEOFBIRTH,COUNTRY,CITY,PLZ," \
            "STREET,GENDER,ETHNIC,RACE".split(",")


class TestErrorRates(TestCase):
    def setUp(self) -> None:
        self.df = pd.read_csv(filepath, names=col_names, dtype={"PLZ": str, "YEAROFBIRTH": int}, keep_default_na=False)
        self.attrs = get_attrs(col_names)

    def _test_values(self, min_e, max_e, measure: callable):
        df_filtered = filter_by_error_rate(self.df, min_e, max_e, measure)
        errors = get_all_errors(df_filtered, measure)
        # check that all errors are correct
        self.assertTrue(errors.map(lambda e: min_e <= e <= max_e).all())
        # TODO check that no non-matches were deleted
        # TODO check with preserve_overlap=True that the overlap is preserved

    def _test_measure(self, measure):
        highest_e = get_all_errors(self.df, measure).max()
        # check that all errors from 0 to the highest occurring error are filtered correctly
        for min_e in range(0, ceil(highest_e) + 1):
            for max_e in range(min_e, ceil(highest_e) + 1):
                self._test_values(min_e, max_e, measure)

    def test_filter_by_error_rate(self):
        self._test_measure(lambda row: count_errors(row, self.attrs))
        self._test_measure(lambda row: avg_edit_distance(row, self.attrs))


if __name__ == '__main__':
    main()
