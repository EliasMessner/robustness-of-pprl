import numpy as np
import nltk


def count_errors(row, attrs, suffixes=None):
    if suffixes is None:
        suffixes = ["_a", "_b"]
    errors = 0
    for attr in attrs:
        if row[attr + suffixes[0]] != row[attr + suffixes[1]]:
            errors += 1
    return errors


def avg_edit_distance(row, attrs, suffixes=None):
    if suffixes is None:
        suffixes = ["_a", "_b"]
    return np.mean(
        [nltk.edit_distance(str(row[attr + suffixes[0]]), str(row[attr + suffixes[1]]))
         for attr in attrs]
    )
