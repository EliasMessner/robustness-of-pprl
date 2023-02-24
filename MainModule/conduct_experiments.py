import itertools
import logging
import os.path
import shutil
import subprocess
from pathlib import Path
from datetime import datetime as dt

import mlflow
from tqdm import tqdm

from constants import *
from eval_adapter import EvalAdapter
from tracker import Tracker
from util import read_json, write_json, get_config_path_from_argv, read_txt, list_folder_names


def main():
    prepare_logger()
    # create outfile for matching result if not exists
    Path(matchings_dir).mkdir(parents=True, exist_ok=True)
    shutil.rmtree(matchings_dir, ignore_errors=True)  # delete existing matching
    _exp_config_path = get_config_path_from_argv(default=default_exp_config_path)
    print(f"Conducting experiments based on {_exp_config_path}")
    # get configs for all experiments
    experiments = read_json(_exp_config_path)["experiments"]
    # for each experiment, there is a dict of parameters for the RLModule
    tracker = Tracker()
    for exp_params in tqdm(experiments, desc="Experiments", bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}'):
        conduct_experiment(exp_params, tracker)


def prepare_logger():
    logs_sub_dir = os.path.join(logs_dir, "conduct_experiments")
    Path(logs_sub_dir).mkdir(parents=True, exist_ok=True)
    timestamp = dt.now().strftime("%Y-%m-%d_%H-%M-%S")
    logging.basicConfig(filename=os.path.join(logs_sub_dir, f"{timestamp}.txt"),
                        filemode='a',
                        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.INFO)


def conduct_experiment(exp_params, tracker):
    tracker.start_exp(exp_params, exp_tags=get_exp_tags())
    exp_no = exp_params.pop("exp_no")
    # create folder for this experiment's matching results
    exp_out_folder = os.path.join(matchings_dir, f"exp_{exp_no}")
    Path(exp_out_folder).mkdir(exist_ok=True, parents=True)
    rl_configs = get_rl_configs(exp_params)
    variant_groups = list_folder_names(dataset_variants_dir)  # one run per rl_config per dataset_variant
    for variant_group in tqdm(variant_groups, desc="Run-Groups", bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}',
                              leave=False):
        variant_group_path = os.path.join(dataset_variants_dir, variant_group)
        variants = list_folder_names(variant_group_path)
        parent_run_description = read_txt(os.path.normpath(os.path.join(variant_group_path, "desc.txt")))
        with mlflow.start_run(experiment_id=tracker.get_current_exp_id(), nested=True,
                              description=parent_run_description):
            for rl_config, variant_folder_name in tqdm(list(itertools.product(rl_configs, variants)),
                                                       desc="Runs",
                                                       bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}',
                                                       leave=False):
                variant_folder_name = os.path.join(variant_group, variant_folder_name)
                conduct_run(exp_out_folder, rl_config, tracker, variant_folder_name)


def get_exp_tags():
    _dm_config_path = os.path.join(dataset_variants_dir, "dm_config.json")
    desc = read_json(_dm_config_path).get("desc", None)
    base_dataset = read_json(_dm_config_path)["base_dataset"]
    exp_tags = {"base_dataset": base_dataset, "desc": desc}
    return exp_tags


def conduct_run(exp_out_folder, rl_config, tracker, variant_folder_name):
    data_path = os.path.join(dataset_variants_dir, variant_folder_name, "records.csv")
    run_out_folder = os.path.join(exp_out_folder, variant_folder_name)
    outfile_path = os.path.join(run_out_folder, "matching.csv")
    # create rl_config.json in the folder for this run
    rl_config_abs_path = write_record_linkage_config(rl_config, run_out_folder)
    call_rl_module(os.path.abspath(data_path), os.path.abspath(outfile_path), rl_config_abs_path, rl_config,
                   tracker)


def get_rl_configs(exp_params):
    """
    In case there is a list of seeds (=token-salting affixes) given, make one record-linkage-config for each value in
    the list.
    """
    rl_configs = []
    if isinstance(exp_params["seed"], list):
        for seed in exp_params["seed"]:
            new_rl_config = exp_params.copy()
            new_rl_config["seed"] = seed
            rl_configs.append(new_rl_config)
    else:
        rl_configs.append(exp_params)
    return rl_configs


def write_record_linkage_config(rl_config: dict, run_out_folder: str) -> str:
    """
    create config for rl module for this run and return its absolute path
    """
    Path(run_out_folder).mkdir(exist_ok=True, parents=True)
    rl_config_path = os.path.join(run_out_folder, "rl_config.json")
    write_json(rl_config, rl_config_path)
    return os.path.abspath(rl_config_path)


def call_rl_module(data_path, outfile_path, rl_config_path: str, rl_config: dict, tracker: Tracker):
    """
    Call RLModule with given parameters.
    Evaluate and track run.
    """
    with mlflow.start_run(experiment_id=tracker.get_current_exp_id(), nested=True):
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
            logging.exception(output)
            raise e
        # evaluate and track the run
        _dm_config_path = os.path.join(dataset_variants_dir, "dm_config.json")
        data_clm_names = read_json(_dm_config_path)["col_names"]
        eval_adapter = EvalAdapter(data_path, data_clm_names=data_clm_names, pred_path=outfile_path)
        dv_params = read_json(os.path.normpath(os.path.join(data_path, "..", "params.json")))
        desc_path = os.path.normpath(os.path.join(data_path, "..", "..", "desc.txt"))
        mlflow.log_metrics(eval_adapter.metrics())
        mlflow.log_params(dv_params)
        mlflow.set_tags(rl_config)
        mlflow.set_tag("base_dataset", read_json(_dm_config_path)["base_dataset"])
        mlflow.log_artifact(_dm_config_path)
        mlflow.log_artifact(data_path)  # records.csv
        mlflow.log_artifact(outfile_path)  # matching.csv
        mlflow.log_artifact(rl_config_path)  # rl_config.json
        mlflow.log_artifact(desc_path)  # desc.txt


if __name__ == "__main__":
    main()
