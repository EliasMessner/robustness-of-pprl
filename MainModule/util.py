import json


def read_json(path):
    with open(path, "r") as f:
        json_object = json.load(f)
    return json_object


def write_json(data, path, mode="w", indent=2):
    with open(path, mode) as f:
        json.dump(data, f, indent=indent)
