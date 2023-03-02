import os.path

from util import list_folder_names

# change artifact path prefixes for all runs in given experiments

old_path_prefix = "C:/Users/elias/PycharmProjects"
new_path_prefix = "home/elias/VSCodeProjects"

exp_ids = ["308707501760303082", "841125091019294509", "382333411211593299", "472950231398647616"]


def main():
    for exp_id in exp_ids:
        exp_path = os.path.join("mlruns", exp_id)
        replace(os.path.join(exp_path, "meta.yaml"))
        for run_folder in list_folder_names(exp_path):
            replace(os.path.join(exp_path, run_folder, "meta.yaml"))


def replace(test_path):
    with open(test_path, "r") as file:
        data = file.read()
        new_data = data.replace(old_path_prefix, new_path_prefix)
    with open(test_path, "w") as file:
        file.write(new_data)


if __name__ == "__main__":
    main()
