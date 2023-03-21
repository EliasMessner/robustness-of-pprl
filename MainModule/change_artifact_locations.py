"""
Utility script to aid with cross-platform use.
Changes artifact path prefixes for all runs in given experiments.
"""

import os.path

from util import list_folder_names

old_path_prefix = "C:/Users/elias/PycharmProjects"
new_path_prefix = "home/elias/VSCodeProjects"

exp_ids = ['566448779314245585',
           '175370998094327392',
           '472750149494883633',
           '514570775026911568',
           '903781682271195311',
           '832774668117266877',
           '412863229096719768',
           '502926108023846195',
           '927827064852785013']


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
