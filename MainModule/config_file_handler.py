import json
import os
from pathlib import Path


class ConfigFileHandler:
    def __init__(self, exp_config_path, run_configs_path):
        """
        :param exp_config_path: path for experiment config file. A single file containing all info about all experiments
        :param run_configs_path: dir path where all run configs should be stored. For each experiment, a folder is
        created containing all run config files for this experiment. The folder is named after the exp id.
        """
        self.exp_config_path = exp_config_path
        self.run_configs_path = run_configs_path

    def create_experiment_config(self):
        """
        Create experiment config json file, containing all the information about the experiments and runs to be executed.
        """
        # TODO create dict by combining values from given parameter lists
        runs = [
            {"id": 0, "subsetSelector": "RANDOM", "size": 20000, "seed": 1, "overlap": 0.1},
            {"id": 1, "subsetSelector": "RANDOM", "size": 20000, "seed": 1, "overlap": 0.2},
            {"id": 2, "subsetSelector": "RANDOM", "size": 20000, "seed": 1, "overlap": 0.3}
        ]
        config = {"experiments": [
            {"id": 0, "seed": 1, "l": 1000, "k": 10, "t": 0.6,
             "runs": runs},
            {"id": 1, "seed": 1, "l": 1000, "k": 10, "t": 0.7,
             "runs": runs}
        ]}
        with open(self.exp_config_path, "w") as file:
            json.dump(config, file)

    def create_run_configs(self):
        """
        Read experiment config file, for each exp create a folder, for each run create a run file which can later be passed
        as parameter to the RLModule.
        """
        Path(self.run_configs_path).mkdir(parents=True, exist_ok=True)
        with open(self.exp_config_path, "r") as file:
            exp_config = json.load(file)
        for exp in exp_config["experiments"]:
            # create folder for each experiment, named with the id of the exp
            exp_dir = os.path.join(self.run_configs_path, str(exp["id"]))
            Path(exp_dir).mkdir(parents=True, exist_ok=True)
            for i, run_config in enumerate(exp["runs"]):
                # store info about each run in a json file
                filename = f"run_{i}.json"
                dest_path = os.path.join(exp_dir, filename)
                with open(dest_path, "w") as file:
                    json.dump(run_config, file)