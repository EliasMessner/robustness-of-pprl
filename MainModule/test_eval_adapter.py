from unittest import TestCase
import pandas as pd
import recordlinkage as rl

from eval_adapter import prepare_for_eval


class Test(TestCase):
    def setUp(self) -> None:
        self.data_path = "data/test_eval_data.csv"
        self.data_clm_names = ["sourceID", "globalID"]
        self.pred_path = "data/test_eval_pred.csv"

    def test_prepare_for_eval(self):
        links_true, links_pred, total = prepare_for_eval(self.data_path, self.data_clm_names, self.pred_path)
        pd.testing.assert_frame_equal(links_true, pd.DataFrame.from_dict({
            "globalID_A": [3, 4],
            "globalID_B": [3, 4]
        }).set_index(["globalID_A", "globalID_B"]))
        pd.testing.assert_frame_equal(links_pred, pd.DataFrame.from_dict({
            "globalID_A": [1, 3, 2],
            "globalID_B": [4, 3, 5]
        }).set_index(["globalID_A", "globalID_B"]))
        self.assertEqual(total, 16)

    def test_metrics(self):
        links_true, links_pred, total = prepare_for_eval(self.data_path, self.data_clm_names, self.pred_path)
        exp_tp = 1
        exp_fp = 2
        exp_tn = 12
        exp_fn = 1
        exp_p = 1/3
        exp_r = 1/2
        obs_tp = rl.true_positives(links_true, links_pred)
        obs_fp = rl.false_positives(links_true, links_pred)
        obs_tn = rl.true_negatives(links_true, links_pred, total)
        obs_fn = rl.false_negatives(links_true, links_pred)
        obs_p = rl.precision(links_true, links_pred)
        obs_r = rl.recall(links_true, links_pred)
        self.assertEqual(exp_tp, obs_tp)
        self.assertEqual(exp_fp, obs_fp)
        self.assertEqual(exp_tn, obs_tn)
        self.assertEqual(exp_fn, obs_fn)
        self.assertEqual(exp_p, obs_p)
        self.assertEqual(exp_r, obs_r)
