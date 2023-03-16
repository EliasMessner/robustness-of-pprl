import itertools
import os
import shutil
import subprocess
from pathlib import Path
import logging

from tqdm import tqdm

from constants import matchings_dir, default_rl_config_path, dataset_variants_dir, pprl_storage_file_location, \
    rl_params_file_name
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
    iterate_rl_param_variations(rl_base_config_path)


def iterate_rl_param_variations(rl_base_config_path):
    rl_param_variations = resolve_rl_config(read_json(rl_base_config_path))
    for rl_params_no, rl_params in tqdm(list(enumerate(rl_param_variations)), desc="RL-params"):
        rl_params_folder = os.path.join(matchings_dir, f"rl_params_{rl_params_no}")
        Path(rl_params_folder).mkdir(exist_ok=True, parents=True)
        iterate_variants(rl_params, rl_params_folder)


def resolve_rl_config(rl_base_config: dict):
    """
    Resolve the rl base config into the individual rl param variations.
    E.g., in case there is a list of seeds (=token-salting affixes) given, make one rl-param variation for each value in
    the list.
    """
    rl_base_config_val_lists = {key: value if isinstance(value, list) else [value]
                                for key, value in
                                rl_base_config.items()}  # convert all values to lists if they aren't already
    rl_param_vars = []
    for value_combination in itertools.product(*rl_base_config_val_lists.values()):
        new_rl_param_var = {key: value for key, value in zip(rl_base_config_val_lists.keys(), value_combination)}
        rl_param_vars.append(new_rl_param_var)
    return rl_param_vars


def iterate_variants(rl_params, rl_params_folder):
    rl_params_file_path = write_rl_params_file(rl_params, rl_params_folder)
    for variant_name in tqdm(list_folder_names_flattened(dataset_variants_dir), desc="Variants", leave=False):
        data_path = os.path.join(dataset_variants_dir, variant_name, "records.csv")
        outfile_path = os.path.join(rl_params_folder, variant_name, "matching.csv")
        call_rl_module(data_path, outfile_path, rl_params_file_path)


def write_rl_params_file(rl_params: dict, out_folder: str) -> str:
    """
    create JSON file with params for rl module for this run and return its path
    """
    Path(out_folder).mkdir(exist_ok=True, parents=True)
    rl_params_path = os.path.join(out_folder, rl_params_file_name)
    write_json(rl_params, rl_params_path)
    return rl_params_path


def call_rl_module(data_path, outfile_path, rl_params_path):
    cmd = ["java", "-jar", "../RLModule/target/RLModule.jar",
           "-d", data_path,
           "-o", outfile_path,
           "-c", rl_params_path,
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
