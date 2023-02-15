from unittest import TestCase

import pandas as pd

from attr_val_dist import get_max_possible_size


class Test(TestCase):
    def test_get_max_possible_size(self):
        col = "GENDER"
        df = pd.DataFrame.from_dict({col: ["M", "M", "M", "M", "M", "M",
                                           "F", "F", "F", "F"]})
        desired_distr = {"M": 0.5, "F": 0.5}
        expected = 8
        observed = get_max_possible_size(desired_distr, df, col)
        self.assertEqual(expected, observed)

        desired_distr = {"M": 0.6, "F": 0.4}
        expected = 10
        observed = get_max_possible_size(desired_distr, df, col)
        self.assertEqual(expected, observed)

        desired_distr = {"M": 0.55, "F": 0.45}
        expected = 8
        observed = get_max_possible_size(desired_distr, df, col)
        self.assertEqual(expected, observed)

        df = pd.DataFrame.from_dict({col: ["M", "M", "M", "M", "M", "M",
                                           "F", "F", "F",
                                           "U", "U", "U", "U", "U", "U"]})
        desired_distr = {"M": 0.4, "F": 0.2, "U": 0.4}
        expected = 15
        observed = get_max_possible_size(desired_distr, df, col)
        self.assertEqual(expected, observed)

        desired_distr = {"M": 0.8, "F": 0.1, "U": 0.1}
        expected = 7
        observed = get_max_possible_size(desired_distr, df, col)
        self.assertEqual(expected, observed)
