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


def write_file(path, content):
    with open(path, 'w') as file:
        file.write(content)


def get_config_path_from_argv(default=None, required=True):
    """
    Check if config path was passed as command line argument,
    if yes return it, otherwise return the default value, or raise Exception if required=True.
    Raise Exception if more than one command line args are given.
    """
    if len(argv) == 1:
        if required:
            raise ValueError("No config file path specified, but it is required.")
        return default
    if len(argv) == 2:
        return argv[1]
    raise ValueError("Too many command line arguments.")


def list_folder_names(dir_path) -> list:
    return sorted(next(os.walk(dir_path))[1])


def list_folder_names_flattened(dir_path):
    """
    Concatenate the names of sub folders of each folder in given directory and return them as list.
    Example:
        dir
        |-folder1
            |-sub1
            |-sub2
        |-folder2
            |-sub3
            |-sub4

        Returns ["folder1/sub1", "folder1/sub2", "folder2/sub3", "folder2/sub4"]
    """
    result = []
    for sub_folder in list_folder_names(dir_path):
        result += [os.path.join(sub_folder, folder_name)
                   for folder_name in list_folder_names(os.path.join(dir_path, sub_folder))]
    return result


def list_file_paths(dir_path):
    """
    return an alphabetically sorted list of all files (their paths) in given directory. Omit folders.
    """
    return [os.path.join(dir_path, file_name) for file_name in sorted(next(os.walk(dir_path))[2])]
