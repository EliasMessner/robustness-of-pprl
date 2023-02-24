import itertools
import os
import shutil
import subprocess
from pathlib import Path
from datetime import datetime as dt
import logging

from tqdm import tqdm

from constants import logs_dir, matchings_dir, default_rl_config_path, dataset_variants_dir, pprl_storage_file_location
from util import get_config_path_from_argv, read_json, list_folder_names, write_json


def main():
    prepare_logger()
    # create outfile for matching result if not exists
    Path(matchings_dir).mkdir(parents=True, exist_ok=True)
    shutil.rmtree(matchings_dir, ignore_errors=True)  # delete existing matching
    rl_config_path = get_config_path_from_argv(default=default_rl_config_path)
    print(f"Conducting matching based on {rl_config_path}")
    # get configs for all experiments
    rl_config_list = read_json(rl_config_path)["experiments"]  # TODO change key name
    for rl_config in tqdm(rl_config_list, desc="Matching", bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}'):
        iterate_variant_groups(rl_config)


def prepare_logger():
    logs_sub_dir = os.path.join(logs_dir, "conduct_matching")
    Path(logs_sub_dir).mkdir(parents=True, exist_ok=True)
    timestamp = dt.now().strftime("%Y-%m-%d_%H-%M-%S")
    logging.basicConfig(filename=os.path.join(logs_sub_dir, f"{timestamp}.txt"),
                        filemode='a',
                        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.INFO)


def iterate_variant_groups(rl_config):
    exp_no = rl_config['exp_no']
    timestamp = dt.now().strftime("%Y-%m-%d_%H:%M:%S")
    exp_name = f"{exp_no}_{timestamp}"
    # create folder for this experiment's matching results
    exp_out_folder = os.path.join(matchings_dir, exp_name)
    Path(exp_out_folder).mkdir(exist_ok=True, parents=True)
    rl_param_list = resolve_rl_configs(rl_config)
    v_group_folder_names = list_folder_names(dataset_variants_dir)  # variant group folder names
    for v_group_folder_name in tqdm(v_group_folder_names, desc="Variant-Groups",
                                    bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}',
                                    leave=False):
        do_matching_for_variant_group(rl_param_list, v_group_folder_name, exp_out_folder)


def resolve_rl_configs(exp_params):
    """
    In case there is a list of seeds (=token-salting affixes) given, make one record-linkage-config for each value in
    the list.
    """
    rl_param_list = []
    if isinstance(exp_params["seed"], list):
        for seed in exp_params["seed"]:
            new_rl_config = exp_params.copy()
            new_rl_config["seed"] = seed
            rl_param_list.append(new_rl_config)
    else:
        rl_param_list.append(exp_params)
    return rl_param_list


def do_matching_for_variant_group(rl_param_list, v_group_folder_name, exp_out_folder):
    variant_group_path = os.path.join(dataset_variants_dir, v_group_folder_name)
    variant_folder_names = list_folder_names(variant_group_path)
    for rl_params, variant_folder_name in tqdm(list(itertools.product(rl_param_list, variant_folder_names)),
                                               desc="Variants",
                                               bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}',
                                               leave=False):
        full_variant_folder_name = os.path.join(v_group_folder_name, variant_folder_name)
        data_path = os.path.join(dataset_variants_dir, full_variant_folder_name, "records.csv")
        run_out_folder = os.path.join(exp_out_folder, full_variant_folder_name)
        outfile_path = os.path.join(run_out_folder, "matching.csv")
        rl_config_abs_path = write_rl_config(rl_params, run_out_folder)
        call_rl_module(os.path.abspath(data_path), os.path.abspath(outfile_path), rl_config_abs_path)


def write_rl_config(rl_config: dict, out_folder: str) -> str:
    """
    create config for rl module for this run and return its absolute path
    """
    Path(out_folder).mkdir(exist_ok=True, parents=True)
    rl_config_path = os.path.join(out_folder, "rl_config.json")
    write_json(rl_config, rl_config_path)
    return os.path.abspath(rl_config_path)


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
        raise e
