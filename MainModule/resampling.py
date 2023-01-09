from imblearn.over_sampling import SMOTENC
import pandas as pd
import numpy as np


def dummy_df(shape, columns, dummy_value="DUMMY"):
    data = []
    for row_n in range(shape[0]):
        row = []
        for col_n in range(shape[1]):
            row.append(0.0 if col_n == 3 else dummy_value)
        data.append(row)
    return pd.DataFrame(data, columns=columns)


def smotenc(data: pd.DataFrame, desired_size: int, categorical_features):
    # smotenc is used for ml classification, therefore we need a classification problem.
    # We simulate one by labeling all our existing data as class 1 and adding a dummy class 0,
    # which contains only dummy values. The dummy class has the desired size.
    # Calling smotenc on this data will increase the size of the original data (class 1) to
    # the size of the dummy class. Finally, we can delete all the dummy data and end up with
    # the resampled original data.
    if desired_size <= data.shape[0]:
        raise ValueError("desired_size must be larger than data size for upsampling")
    # create the dummy data
    X = pd.concat[data, dummy_df(shape=(desired_size, data.shape[1]), columns=data.columns)]
    # create dummy truth values: all real records get 1 and all dummy values get 0
    y = np.append(np.ones(data.shape[0]), np.zeros(desired_size))
    # resample
    sm = SMOTENC(random_state=42, categorical_features=categorical_features)
    data_res, y_res = sm.fit_resample(X, y)
    # remove dummy records (rows with class 0)
    mask = [bool(x) for x in y_res.tolist()]
    data_res = data_res[mask]
    return data_res
