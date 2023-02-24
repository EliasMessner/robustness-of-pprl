"""
For launching dataset_modifier and conduct_experiments subsequently with given filenames
"""

import subprocess as sp


def main():
    run("dm_config_error_rate", "rl_config")
    run("dm_config_attr_val_length", "rl_config")


def run(dm, ec):
    sp.run(f"python dataset_modifier.py data/{dm}.json", shell=True, check=True)
    sp.run(f"python conduct_matching.py data/{ec}.json", shell=True, check=True)
    sp.run("python evaluate.py", shell=True, check=True)


if __name__ == "__main__":
    main()
