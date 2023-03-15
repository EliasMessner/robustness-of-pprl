"""
Launches all experiment config files in experiment configs dir (configs/exp).
Skips all files whose names are listed in skip.txt.
"""
import os.path

import launch_experiments
from constants import exp_configs_dir
from util import list_file_paths


def main():
    files_to_skip = get_files_to_skip()
    print(f"Skipping {len(files_to_skip)} files: {files_to_skip}")
    for exp_config_path in list_file_paths(exp_configs_dir):
        if any(exp_config_path.endswith(f) for f in files_to_skip):
            continue
        launch_experiments.main(exp_config_path)


def get_files_to_skip():
    skip_txt = "skip.txt"
    try:
        with open(os.path.join(exp_configs_dir, skip_txt)) as file:
            return file.readlines() + [skip_txt]
    except FileNotFoundError:
        return [skip_txt]


if __name__ == "__main__":
    main()
