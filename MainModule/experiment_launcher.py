import itertools
import os
import shutil
import subprocess
from pathlib import Path
from datetime import datetime as dt
import logging

from tqdm import tqdm

from constants import logs_dir, matchings_dir, default_exp_config_path, dataset_variants_dir, pprl_storage_file_location
from util import get_config_path_from_argv, read_json, list_folder_names, write_json


def main():
    prepare_logger()
    # create outfile for matching result if not exists
    Path(matchings_dir).mkdir(parents=True, exist_ok=True)
    shutil.rmtree(matchings_dir, ignore_errors=True)  # delete existing matching
    exp_base_config_path = get_config_path_from_argv(default=default_exp_config_path)
    print(f"Conducting matching based on {exp_base_config_path}")
    # get configs for all experiments
    exp_configs = read_json(exp_base_config_path)["experiments"]
    for exp_config in tqdm(exp_configs, desc="Experiments", bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}'):
        iterate_variant_groups(exp_config)


def prepare_logger():
    logs_sub_dir = os.path.join(logs_dir, "conduct_matching")
    Path(logs_sub_dir).mkdir(parents=True, exist_ok=True)
    timestamp = dt.now().strftime("%Y-%m-%d_%H-%M-%S")
    logging.basicConfig(filename=os.path.join(logs_sub_dir, f"{timestamp}.txt"),
                        filemode='a',
                        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.INFO)


def iterate_variant_groups(exp_config):
    exp_no = exp_config['exp_no']
    timestamp = dt.now().strftime("%Y-%m-%d_%H:%M:%S")
    exp_name = f"{exp_no}_{timestamp}"
    # create folder for this experiment's matching results
    exp_out_folder = os.path.join(matchings_dir, exp_name)
    Path(exp_out_folder).mkdir(exist_ok=True, parents=True)
    rl_configs = resolve_exp_config(exp_config)
    v_group_folder_names = list_folder_names(dataset_variants_dir)  # variant group folder names
    for v_group_folder_name in tqdm(v_group_folder_names, desc="Variant-Groups",
                                    bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}',
                                    leave=False):
        do_matching_for_variant_group(rl_configs, v_group_folder_name, exp_out_folder)


def resolve_exp_config(exp_config: dict):
    """
    Resolve the experiment config into the individual rl configurations.
    E.g., in case there is a list of seeds (=token-salting affixes) given, make one rl-config for each value in
    the list.
    """
    exp_config_val_lists = {key: value if isinstance(value, list) else [value]
                            for key, value in exp_config.items()}  # convert all values to lists if they aren't already
    rl_configs = []
    for value_combination in itertools.product(*exp_config_val_lists.values()):
        new_rl_config = exp_config.copy()
        for key, value in zip(exp_config_val_lists.keys(), value_combination):
            new_rl_config[key] = value
        rl_configs.append(new_rl_config)
    return rl_configs


def do_matching_for_variant_group(rl_configs, v_group_folder_name, exp_out_folder):
    variant_group_path = os.path.join(dataset_variants_dir, v_group_folder_name)
    variant_folder_names = list_folder_names(variant_group_path)
    for variant_folder_name in tqdm(variant_folder_names, desc="Variants",
                                    bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}', leave=False):
        full_variant_folder_name = os.path.join(v_group_folder_name, variant_folder_name)
        data_path = os.path.join(dataset_variants_dir, full_variant_folder_name, "records.csv")
        for rl_config_no, rl_config in tqdm(list(enumerate(rl_configs)),
                                            desc="RL-configs",
                                            bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}',
                                            leave=False):
            run_out_folder = os.path.join(exp_out_folder, full_variant_folder_name, f"rl_config_{rl_config_no}")
            outfile_path = os.path.join(run_out_folder, "matching.csv")
            rl_config_abs_path = write_rl_config(rl_config, run_out_folder)
            call_rl_module(os.path.abspath(data_path), os.path.abspath(outfile_path), rl_config_abs_path)


def write_rl_config(rl_config: dict, out_folder: str) -> str:
    """
    create config for rl module for this run and return its absolute path
    """
    Path(out_folder).mkdir(exist_ok=True, parents=True)
    rl_config_path = os.path.join(out_folder, "exp_config.json")
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


if __name__ == "__main__":
    main()
