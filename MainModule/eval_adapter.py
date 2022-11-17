import pandas as pd
import recordlinkage as rl


class EvalAdapter:
    def __init__(self, data_path, data_clm_names, pred_path,
                 id_clm="globalID", src_clm="sourceID",
                 pred_clm_a="globalID_A", pred_clm_b="globalID_B") -> (pd.DataFrame, pd.DataFrame, int):
        """
        Read dataset and predicted matches from files, generate true matches from data,
        prepare the data needed for evaluation with third party recordlinkage module.
        https://recordlinkage.readthedocs.io/en/latest/ref-evaluation.html
        """
        data = pd.read_csv(data_path, names=data_clm_names)
        d1, d2 = [x for _, x in data.groupby(src_clm)]
        ids_true = pd.merge(d1, d2, how="inner", on=[id_clm])[id_clm]
        self.links_true = pd.concat([ids_true, ids_true], axis=1)
        self.links_true.columns = [pred_clm_a, pred_clm_b]
        self.links_true.set_index([pred_clm_a, pred_clm_b], inplace=True)
        self.links_pred = pd.read_csv(pred_path, names=[pred_clm_a, pred_clm_b], index_col=[pred_clm_a, pred_clm_b])
        self.total = d1.shape[0] * d2.shape[0]  # total number of possible record pairs

    def precision(self):
        return rl.precision(self.links_true, self.links_pred)

    def recall(self):
        return rl.recall(self.links_true, self.links_pred)

    def fscore(self):
        return rl.fscore(self.links_true, self.links_pred)

    def accuracy(self):
        return rl.accuracy(self.links_true, self.links_pred, self.total)
