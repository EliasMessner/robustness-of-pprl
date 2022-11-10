import pandas as pd
from typing import Tuple


class SubsetGenerator:
    def __init__(self, filepath: str, col_names: list, source_id_col_name: str="sourceID", global_id_col_name: str="globalID"):
        """
        The passed dataset is expected to consist of two equally sized sources, 
        distinguishable by a column containing a sourceID.

        :param filepath: Filepath to dataset csv
        :type filepath: str
        :param col_names: column names of dataset
        :type col_names: list of str
        :param source_id_col_name: name of the column containing the two sourceID (e.g. A and B), defaults to "sourceID"
        :type source_id_col_name: str
        :param global_id_col_name: name of column containing global ID, defaults to "globalID"
        :type global_id_col_name: str
        """
        self.df = pd.read_csv(filepath, names=col_names)
        self.df1, self.df2 = [x for _, x in self.df.groupby(source_id_col_name)]
        assert self.df1.shape == self.df2.shape
        self.global_id_col_name = global_id_col_name
        self.source_id_col_name = source_id_col_name
        self.true_matches1, self.true_matches2 = self._get_true_matches()
        self._base_overlap = self._get_base_overlap()

    def random_sample(self, size: int, seed: int, overlap: float=None) -> pd.DataFrame:
        """
        Draw random sample from base dataset.

        :param size: number of records to draw from each of the two sources.
        :type size: int
        :param overlap: ratio of true matches to whole size of one source, if not specified the ratio will be the same as in the base dataset.
        :type overlap: float
        :raises ValueError: if size not between 0 and the size of either of the sources
        :raises ValueError: if overlap not between 0 and maximally possible overlap wrt. to sample size
        :return: random sample
        :rtype: pd.DataFrame
        """
        if not (0 <= size <= self.df1.shape[0]):
            raise ValueError(f"Size must be between 0 and size of one of the two source data sets (={self.df1.shape[0]}). Got {size} instead.")
        rel_sample_size = size / self.df.shape[0]
        max_overlap = min(1.0, self._base_overlap / rel_sample_size)
        if overlap is None:
            overlap = self._base_overlap
        if not (0 <= overlap <= max_overlap or overlap is None):
            raise ValueError(f"Overlap must be between 0 and {max_overlap}. Got {overlap} instead.")
        # draw size*overlap from true matches of source A
        a_matches = self.true_matches1.sample(round(size * overlap), random_state=seed)
        # draw size*(1-overlap) from non-matches of source A
        a_non_matches = self.df1[~self.df1[self.global_id_col_name].isin(self.true_matches1[self.global_id_col_name])] \
            .sample(round(size * (1 - overlap)), random_state=seed)
        # draw size*overlap from true matches of source B
        b_matches = self.true_matches2.sample(round(size * overlap), random_state=seed)
        # draw size*(1-overlap) from non-matches of source B
        b_non_matches = self.df2[~self.df2[self.global_id_col_name].isin(self.true_matches2[self.global_id_col_name])] \
            .sample(round(size * (1 - overlap)), random_state=seed)
        # concatenate all
        return pd.concat([a_matches, a_non_matches, b_matches, b_non_matches])

    def _get_true_matches(self) -> Tuple[pd.DataFrame]:
        true_matches_1 = self.df1[self.df1[self.global_id_col_name].isin(self.df2[self.global_id_col_name])]
        true_matches_2 = self.df2[self.df2[self.global_id_col_name].isin(self.df1[self.global_id_col_name])]
        return true_matches_1, true_matches_2

    def _get_base_overlap(self):
        """
        Returns overlap in base dataset. Overlap is calculated wrt. size of one source, 
        which means that if there are two datasets A and B with each 100 records,
        20 of which are matches and 80 non-matches, then the overlap will be 0.2.

        :return: Overlap value between 0 and 1.
        :rtype: float
        """
        intersect = pd.merge(self.df1, self.df2, how="inner", on=[self.global_id_col_name])
        return 2 * intersect.shape[0] / self.df.shape[0]