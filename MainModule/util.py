import json
import os
from sys import argv


def read_json(path) -> dict:
    with open(path, "r") as f:
        json_object = json.load(f)
    return json_object


def write_json(data, path, mode="w", indent=2):
    with open(path, mode) as f:
        json.dump(data, f, indent=indent)


def read_txt(path) -> str:
    return "".join(open(path, "r").readlines())


def get_config_path_from_argv(default: str):
    """
    Check if config path was passed as command line argument,
    if yes return it, otherwise return the default value.
    Raise Exception if too many command line args are given.
    """
    if len(argv) == 1:
        return default
    if len(argv) == 2:
        return argv[1]
    raise ValueError("Too many command line arguments.")


def list_folder_names(dir_path):
    return next(os.walk(dir_path))[1]
