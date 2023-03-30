"""
Utility script to aid with cross-platform use.
Changes artifact path prefixes for all runs in given experiments.
"""

import os.path

from util import list_folder_names

old_path_prefix = "C:/Users/elias/PycharmProjects"
new_path_prefix = "home/elias/VSCodeProjects"

exp_ids = ['922712977179527982',
           '757048666589841847',
           '371543923936467803',
           '904308714702312490',
           '558487971098743689',
           '458281253693485184',
           '324648064308263052',
           '125949732931127323']


def main():
    for exp_id in exp_ids:
        exp_path = os.path.join("mlruns", exp_id)
        replace(os.path.join(exp_path, "meta.yaml"))
        for run_folder in list_folder_names(exp_path):
            replace(os.path.join(exp_path, run_folder, "meta.yaml"))


def replace(test_path):
    with open(test_path, "r") as file:
        data = file.read()
        new_data = data.replace(old_path_prefix, new_path_prefix)
    with open(test_path, "w") as file:
        file.write(new_data)


if __name__ == "__main__":
    main()
