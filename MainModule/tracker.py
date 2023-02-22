from datetime import datetime as dt
import mlflow


class Tracker:
    def __init__(self):
        self.timestamp = dt.now().strftime("%Y-%m-%d_%H:%M:%S")
        self.current_exp = None

    def start_exp(self, exp_params, exp_tags: dict = None):
        exp_no = exp_params["exp_no"]
        if exp_params.get("id", None) is None:
            exp_id = mlflow.create_experiment(name=f"{exp_no}_{self.timestamp}",
                                              tags=exp_tags)
            self.current_exp = mlflow.get_experiment(exp_id)
        else:
            # TODO check if such an experiment exists in backend store
            self.current_exp = mlflow.get_experiment(exp_params["id"])

    def get_current_exp_id(self):
        return self.current_exp.experiment_id
