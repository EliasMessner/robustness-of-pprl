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
from util import read_json, write_json, get_config_path_from_argv


def main():
    prepare_logger()
    # create outfile for matching result if not exists
    Path(matchings_dir).mkdir(parents=True, exist_ok=True)
    shutil.rmtree(matchings_dir, ignore_errors=True)  # delete existing matching
    _exp_config_path = get_config_path_from_argv(default=exp_config_path)
    # get configs for all experiments
    experiments = read_json(_exp_config_path)["experiments"]
    # for each experiment, there is a dict of parameters for the RLModule
    tracker = Tracker()
    for exp_params in tqdm(experiments, desc="Experiments", bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}'):
        conduct_experiment(exp_params, tracker)


def prepare_logger():
    Path(logs_dir).mkdir(parents=True, exist_ok=True)
    timestamp = dt.now().strftime("%Y-%m-%d_%H-%M-%S")
    logging.basicConfig(filename=os.path.join(logs_dir, f"{timestamp}.txt"),
                        filemode='a',
                        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.INFO)


def conduct_experiment(exp_params, tracker):
    tracker.start_exp(exp_params)
    exp_no = exp_params.pop("exp_no")
    # create folder for this experiment's matching results
    exp_out_folder = os.path.join(matchings_dir, f"exp_{exp_no}")
    Path(exp_out_folder).mkdir(exist_ok=True, parents=True)
    matcher_configs = get_matcher_configs(exp_params)
    for matcher_config in matcher_configs:
        variants = os.listdir(dataset_variants_dir)  # one run per dataset variant
        for variant_folder_name in tqdm(variants, desc="Runs", bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}', leave=False):
            conduct_run(exp_out_folder, matcher_config, tracker, variant_folder_name)


def conduct_run(exp_out_folder, matcher_config, tracker, variant_folder_name):
    data_path = os.path.join(dataset_variants_dir, variant_folder_name, "records.csv")
    run_out_folder = os.path.join(exp_out_folder, variant_folder_name)
    outfile_path = os.path.join(run_out_folder, "matching.csv")
    # create matcher_config.json in the folder for this run
    matcher_config_abs_path = write_matcher_config(matcher_config, run_out_folder)
    call_rl_module(os.path.abspath(data_path), os.path.abspath(outfile_path), matcher_config_abs_path,
                   tracker)


def get_matcher_configs(exp_params):
    """
    In case there is a list of seeds (=token-salting affixes) given, make one matcher config for each value in the list.
    """
    matcher_configs = []
    if isinstance(exp_params["seed"], list):
        for seed in exp_params["seed"]:
            new_matcher_config = exp_params.copy()
            new_matcher_config["seed"] = seed
            matcher_configs.append(new_matcher_config)
    else:
        matcher_configs.append(exp_params)
    return matcher_configs


def write_matcher_config(matcher_config: dict, run_out_folder: str) -> str:
    """
    create config for matcher for this run and return its absolute path
    """
    Path(run_out_folder).mkdir(exist_ok=True)
    matcher_config_path = os.path.join(run_out_folder, "config.json")
    write_json(matcher_config, matcher_config_path)
    return os.path.abspath(matcher_config_path)


def call_rl_module(data_path, outfile_path, config_path, tracker: Tracker):
    """
    Call RLModule with given parameters.
    Evaluate and track run.
    """
    with mlflow.start_run(experiment_id=tracker.get_current_exp_id()):
        cmd = ["java", "-jar", "../RLModule/target/RLModule.jar",
               "-d", data_path,
               "-o", outfile_path,
               "-c", config_path,
               "-s", pprl_storage_file_location]
        try:
            output = subprocess.check_output(cmd, encoding='UTF-8')
        except subprocess.CalledProcessError as e:
            print(f"Command: '{' '.join(cmd)}'")
            raise e
        finally:
            logging.info(output)
        # evaluate and track the run
        data_clm_names = read_json(dm_config_path)["col_names"]
        eval_adapter = EvalAdapter(data_path, data_clm_names=data_clm_names, pred_path=outfile_path)
        dv_params = read_json(os.path.normpath(os.path.join(data_path, "..", "params.json")))
        mlflow.log_metrics(eval_adapter.metrics())
        mlflow.log_params(dv_params)
        mlflow.log_artifact(data_path)
        mlflow.log_artifact(outfile_path)
        mlflow.log_artifact(config_path)


if __name__ == "__main__":
    main()
