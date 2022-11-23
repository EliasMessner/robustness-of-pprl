import os.path
import subprocess
from pathlib import Path
from datetime import datetime as dt

import mlflow
from tqdm import tqdm

from constants import *
from eval_adapter import EvalAdapter
from util import read_json, write_json

TIMESTAMP = dt.now().strftime("%Y-%m-%d_%H:%M:%S")


def main():
    # create outfile for matching result if not exists
    Path(matchings_dir).mkdir(parents=True, exist_ok=True)
    # get configs for all experiments
    experiments = read_json(exp_config_path)["experiments"]
    # for each experiment, there is a dict of parameters for the RLModule
    for exp_params in tqdm(experiments, desc="Experiments", bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}'):
        conduct_experiment(exp_params)


def conduct_experiment(exp_params):
    # create folder for this experiment's matching results
    exp_no = exp_params.pop("exp_no")
    exp_out_folder = os.path.join(matchings_dir, f"exp_{exp_no}")
    Path(exp_out_folder).mkdir(exist_ok=True)
    exp_id = mlflow.create_experiment(name=f"{exp_no}_{TIMESTAMP}")
    variants = os.listdir(dataset_variants_dir)  # one run per dataset variant
    for variant_folder_name in tqdm(variants, desc="Variations", bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}', leave=False):
        data_path = os.path.join(dataset_variants_dir, variant_folder_name, "records.csv")
        run_out_folder = os.path.join(exp_out_folder, variant_folder_name)
        outfile_path = os.path.join(run_out_folder, "matching.csv")
        # create matcher_config.json in the folder for this run
        matcher_config_abs_path = create_matcher_config(exp_params, run_out_folder)
        conduct_run(os.path.abspath(data_path), os.path.abspath(outfile_path), matcher_config_abs_path,
                    exp_id, exp_params)


def create_matcher_config(exp_params: dict, run_out_folder: str) -> str:
    """
    create config for matcher for this run and return its absolute path
    """
    Path(run_out_folder).mkdir(exist_ok=True)
    matcher_config_path = os.path.join(run_out_folder, "config.json")
    write_json(exp_params, matcher_config_path)
    return os.path.abspath(matcher_config_path)


def conduct_run(data_path, outfile_path, config_path, exp_id: str, exp_params: dict):
    """
    Call RLModule with given parameters.
    Evaluate and track run.
    """
    with mlflow.start_run(experiment_id=exp_id):
        cmd = ["java", "-jar", "../RLModule/target/RLModule.jar",
               "-d", data_path,
               "-o", outfile_path,
               "-c", config_path]
        try:
            subprocess.check_output(cmd, encoding='UTF-8')
        except subprocess.CalledProcessError as e:
            print(f"Command: '{' '.join(cmd)}'")
            raise e
        # evaluate and track the run
        data_clm_names = read_json(dm_config_path)["col_names"]
        eval_adapter = EvalAdapter(data_path, data_clm_names=data_clm_names, pred_path=outfile_path)
        dv_params = read_json(os.path.normpath(os.path.join(data_path, "..", "params.json")))
        mlflow.log_params(dv_params)
        mlflow.log_metrics(eval_adapter.metrics())
        mlflow.log_artifact(data_path)
        mlflow.log_artifact(outfile_path)
        mlflow.log_artifact(config_path)


if __name__ == "__main__":
    main()
