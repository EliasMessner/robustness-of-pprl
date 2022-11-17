import pandas as pd


def prepare_for_eval(data_path, data_clm_names, pred_path,
                     id_clm="globalID", src_clm="sourceID",
                     pred_clm_a="globalID_A", pred_clm_b="globalID_B") -> (pd.DataFrame, pd.DataFrame, int):
    """
    Read dataset and predicted matches from files, generate true matches from data,
    return dataframes ready for evaluation with third party recordlinkage module.
    https://recordlinkage.readthedocs.io/en/latest/ref-evaluation.html
    :returns: (links_true, links_pred, total)
    total is the total number of possible record pairs, aka. product of the sizes of the two data sources
    """
    data = pd.read_csv(data_path, names=data_clm_names)
    d1, d2 = [x for _, x in data.groupby(src_clm)]
    ids_true = pd.merge(d1, d2, how="inner", on=[id_clm])[id_clm]
    links_true = pd.concat([ids_true, ids_true], axis=1)
    links_true.columns = [pred_clm_a, pred_clm_b]
    links_true.set_index([pred_clm_a, pred_clm_b], inplace=True)
    links_pred = pd.read_csv(pred_path, names=[pred_clm_a, pred_clm_b], index_col=[pred_clm_a, pred_clm_b])
    total = d1.shape[0] * d2.shape[0]
    return links_true, links_pred, total
