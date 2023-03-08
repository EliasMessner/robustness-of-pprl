import itertools
import os
import shutil
import subprocess
from pathlib import Path
import logging

from tqdm import tqdm

from constants import matchings_dir, default_rl_config_path, dataset_variants_dir, pprl_storage_file_location
from log import prepare_logger
from util import get_config_path_from_argv, read_json, write_json, list_folder_names_flattened


def main(rl_base_config_path=None):
    prepare_logger()
    logging.getLogger().setLevel(logging.WARNING)
    # create outfile for matching result if not exists
    shutil.rmtree(matchings_dir, ignore_errors=True)  # delete existing matching
    Path(matchings_dir).mkdir(parents=True, exist_ok=True)
    if rl_base_config_path is None:
        rl_base_config_path = get_config_path_from_argv(default=default_rl_config_path)
    # copy rl_base_config matchings dir on top level, so it can be logged as artifact later
    shutil.copyfile(rl_base_config_path, os.path.join(matchings_dir, "rl_base_config.json"))
    print(f"Conducting matching based on {rl_base_config_path}")
    iterate_rl_configs(rl_base_config_path)


def iterate_rl_configs(rl_base_config_path):
    rl_configs = resolve_rl_config(read_json(rl_base_config_path))
    for rl_config_no, rl_config in tqdm(list(enumerate(rl_configs)), desc="RL-configs"):
        rl_config_folder = os.path.join(matchings_dir, f"rl_config_{rl_config_no}")
        Path(rl_config_folder).mkdir(exist_ok=True, parents=True)
        iterate_variants(rl_config, rl_config_folder)


def resolve_rl_config(rl_base_config: dict):
    """
    Resolve the rl base config into the individual rl configurations.
    E.g., in case there is a list of seeds (=token-salting affixes) given, make one rl-config for each value in
    the list.
    """
    rl_base_config_val_lists = {key: value if isinstance(value, list) else [value]
                                for key, value in
                                rl_base_config.items()}  # convert all values to lists if they aren't already
    rl_configs = []
    for value_combination in itertools.product(*rl_base_config_val_lists.values()):
        new_rl_config = {key: value for key, value in zip(rl_base_config_val_lists.keys(), value_combination)}
        rl_configs.append(new_rl_config)
    return rl_configs


def iterate_variants(rl_config, rl_config_folder):
    rl_config_path = write_rl_config(rl_config, rl_config_folder)
    for variant_name in tqdm(list_folder_names_flattened(dataset_variants_dir), desc="Variants", leave=False):
        data_path = os.path.join(dataset_variants_dir, variant_name, "records.csv")
        outfile_path = os.path.join(rl_config_folder, variant_name, "matching.csv")
        call_rl_module(data_path, outfile_path, rl_config_path)


def write_rl_config(rl_config: dict, out_folder: str) -> str:
    """
    create config for rl module for this run and return its path
    """
    Path(out_folder).mkdir(exist_ok=True, parents=True)
    rl_config_path = os.path.join(out_folder, "rl_config.json")
    write_json(rl_config, rl_config_path)
    return rl_config_path


def call_rl_module(data_path, outfile_path, rl_config_path):
    cmd = ["java", "-jar", "../RLModule/target/RLModule.jar",
           "-d", data_path,
           "-o", outfile_path,
           "-c", rl_config_path,
           "-s", pprl_storage_file_location]
    try:
        output = subprocess.check_output(cmd, encoding='UTF-8')
        logging.info(output)
    except subprocess.CalledProcessError as e:
        print(f"Command: '{' '.join(cmd)}'")
        logging.exception(e)
        logging.exception(e.output)
        raise e


if __name__ == "__main__":
    main()
