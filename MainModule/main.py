"""
For launching dataset_modifier, experiment_launcher, and evaluator subsequently with given filenames
"""

import subprocess as sp


def main():
    run("dm_config_error_rate", "rl_config")
    run("dm_config_attr_val_length", "rl_config")
    run("dm_config_gender", "rl_config")
    run("dm_config_age", "rl_config")
    run("dm_config_plz", "rl_config")
    # run("dm_config_random_cartesian_product", "rl_config_10_seeds")  TODO run this over night


def run(dm_config_name, rl_config_name):
    sp.run(f"python dataset_modifier.py data/{dm_config_name}.json", shell=True, check=True)
    sp.run(f"python conduct_matching.py data/{rl_config_name}.json", shell=True, check=True)
    sp.run("python evaluator.py", shell=True, check=True)


if __name__ == "__main__":
    main()
