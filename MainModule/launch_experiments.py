import create_matching
from constants import default_rl_config_path, dataset_variants_dir
from dataset_modifier import DatasetModifier
from evaluator import Evaluator
from util import get_config_path_from_argv, read_json


def main(exp_config_path=None):
    if exp_config_path is None:
        exp_config_path = get_config_path_from_argv(required=True)
    dm = DatasetModifier()
    evaluator = Evaluator()
    for experiment in read_json(exp_config_path)["experiments"]:
        dm_config_path = experiment['dm_config']
        rl_config_path = experiment.get('rl_config', default_rl_config_path)
        dm.load_dataset_by_config_file(dm_config_path)
        dm.create_variants_by_config_file(dm_config_path, dataset_variants_dir)
        create_matching.main(rl_base_config_path=rl_config_path)
        exp_name = experiment["exp_name"]
        append_if_exists = experiment.get("append_if_exists", None)
        add_timestamp = experiment.get("get_timestamp", None)
        evaluator.evaluate_experiment(exp_name, append_if_exists, add_timestamp)


if __name__ == "__main__":
    main()
