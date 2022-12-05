"""
For Launching dataset_modifier and conduct_experiments subsequently with given filenames
"""

import subprocess as sp


def main():
    run("dataset_modifier_random_only_size", "exp_config")
    run("dataset_modifier_plz_3", "exp_config")


def run(dm, ec):
    sp.run(f"python dataset_modifier.py data/{dm}.json", shell=True, check=True)
    sp.run(f"python conduct_experiments.py data/{ec}.json", shell=True, check=True)


if __name__ == "__main__":
    main()
