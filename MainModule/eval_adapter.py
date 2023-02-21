import pandas as pd
import numpy as np
import recordlinkage as rl


class EvalAdapter:
    def __init__(self, data_path, data_clm_names, pred_path,
                 id_clm="globalID", src_clm="sourceID",
                 pred_clm_a="globalID_A", pred_clm_b="globalID_B"):
        """
        Read dataset and predicted matches from files, generate true matches from data,
        prepare the data needed for evaluation with third party recordlinkage module.
        https://recordlinkage.readthedocs.io/en/latest/ref-evaluation.html
        """
        data = pd.read_csv(data_path, names=data_clm_names)
        d1_and_d2 = [x for _, x in data.groupby(src_clm)]
        if len(d1_and_d2) < 2:
            # if there are no records or only records from one source, no true links exist
            self.links_true = pd.DataFrame(columns=[pred_clm_a, pred_clm_b])  # empty dataframe
            self.total = 0
        else:
            # there is at least one record from each source
            # split into the 2 sources
            d1, d2 = d1_and_d2
            self.total = d1.shape[0] * d2.shape[0]  # total number of possible record pairs
            # get ids of the true links
            ids_true = pd.merge(d1, d2, how="inner", on=[id_clm])[id_clm]
            # concat with itself horizontally, ending up with the same id twice in each row
            self.links_true = pd.concat([ids_true, ids_true], axis=1)
            self.links_true.columns = [pred_clm_a, pred_clm_b]
        self.links_true.set_index([pred_clm_a, pred_clm_b], inplace=True)
        self.links_pred = pd.read_csv(pred_path, names=[pred_clm_a, pred_clm_b], index_col=[pred_clm_a, pred_clm_b])

    def metrics(self):
        # accuracy not important for record linkage
        return {
            "precision": self.precision(),
            "recall": self.recall(),
            "fscore": self.fscore()
        }

    def precision(self):
        if self.links_true.shape[0] == 0:
            return np.nan
        return rl.precision(self.links_true, self.links_pred)

    def recall(self):
        if self.links_true.shape[0] == 0:
            return np.nan
        return rl.recall(self.links_true, self.links_pred)

    def fscore(self):
        if self.links_true.shape[0] == 0:
            return np.nan
        if self.precision() + self.recall() == 0:
            return 0
        return rl.fscore(self.links_true, self.links_pred)

    def accuracy(self):
        if self.total == 0:
            return np.nan
        return rl.accuracy(self.links_true, self.links_pred, self.total)
