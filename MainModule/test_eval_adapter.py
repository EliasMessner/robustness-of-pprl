from unittest import TestCase
import pandas as pd

from eval_adapter import EvalAdapter


class Test(TestCase):
    def setUp(self) -> None:
        self.data_path = "test_resources/eval_data.csv"
        self.data_clm_names = ["sourceID", "globalID"]
        self.pred_path = "test_resources/eval_pred.csv"
        self.eval = EvalAdapter(self.data_path, self.data_clm_names, self.pred_path)

    def test_init(self):
        pd.testing.assert_frame_equal(self.eval.links_true, pd.DataFrame.from_dict({
            "globalID_A": [3, 4],
            "globalID_B": [3, 4]
        }).set_index(["globalID_A", "globalID_B"]))
        pd.testing.assert_frame_equal(self.eval.links_pred, pd.DataFrame.from_dict({
            "globalID_A": [1, 3, 2],
            "globalID_B": [4, 3, 5]
        }).set_index(["globalID_A", "globalID_B"]))
        self.assertEqual(self.eval.total, 16)

    def test_precision(self):
        self.assertEqual(1/3, self.eval.precision())

    def test_recall(self):
        self.assertEqual(1/2, self.eval.recall())

    def test_fscore(self):
        self.assertEqual(2/5, self.eval.fscore())

    def test_accuracy(self):
        self.assertEqual(13/16, self.eval.accuracy())
