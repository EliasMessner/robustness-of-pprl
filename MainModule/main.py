"""
Launches all experiment config files in experiment configs dir (configs/exp)
"""

import launch_experiments
from constants import exp_configs_dir
from util import list_file_paths


def main():
    for exp_config_path in list_file_paths(exp_configs_dir):
        launch_experiments.main(exp_config_path)


if __name__ == "__main__":
    main()
