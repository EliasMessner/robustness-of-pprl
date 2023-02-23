"""
For launching dataset_modifier and conduct_experiments subsequently with given filenames
"""

import subprocess as sp


def main():
    run("dataset_modifier_error_rate", "exp_config")
    run("dataset_modifier_gender", "exp_config")


def run(dm, ec):
    sp.run(f"python dataset_modifier.py data/{dm}.json", shell=True, check=True)
    sp.run(f"python conduct_experiments.py data/{ec}.json", shell=True, check=True)


if __name__ == "__main__":
    main()
