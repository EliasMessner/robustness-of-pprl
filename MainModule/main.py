"""
For launching dataset_modifier, experiment_launcher, and evaluator subsequently with given filenames
"""

import subprocess as sp


def main():
    run("dm_config_error_rate", "exp_config")
    run("dm_config_attr_val_length", "exp_config")


def run(dm, ec):
    sp.run(f"python dataset_modifier.py data/{dm}.json", shell=True, check=True)
    sp.run(f"python conduct_matching.py data/{ec}.json", shell=True, check=True)
    sp.run("python evaluator.py", shell=True, check=True)


if __name__ == "__main__":
    main()
