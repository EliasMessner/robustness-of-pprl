from constants import default_exp_config_path
from util import write_json


def create_experiments_config():
    """
    Create experiment config json file, containing all the information about the experiments and runs to be executed.
    """
    # TODO create dict by combining values from given parameter lists
    config = {"experiments": [
        {"exp_no": 0, "seed": 1, "l": 1000, "k": 10, "t": 0.6},
        {"exp_no": 1, "seed": 1, "l": 1000, "k": 10, "t": 0.7}
    ]}
    write_json(config, default_exp_config_path)