This project is part of my bachelor's thesis at Leipzig University.
Its purpose is to test the robustness of Privacy Preserving Record Linkage against dataset variations.

# RLModule

Requires JDK 17 or newer.

### Build jar
> cd RLModule

> mvn package

### Run
> java -jar target/RLModule.jar -d \<path-to-dataset\> -o \<path-to-outfile\> -c \<path-to-config-file\>

# MainModule

Requires Python 3.9.x

Python 3.10 might cause problems with MLflow.

## Install requirements

If using conda environment, first install pip in conda env using

> conda activate \<some-python-3.9-env\>

> conda install pip

Install requirements

> cd MainModule

> pip install -r requirements.txt

## Standard Use Case

The work flow consists of three successive steps:

1. Create dataset variants

2. Create matching

3. Evaluate results

They can be conducted manually or automatically. For manual use:

> cd MainModule

### Create all Dataset Variants

> python dataset_modifier.py \<optional_dm_config_path\>

<optional_dm_config_path> = path to dataset modifier configuration file, defaults to data/dm_config.json

### Create Matching

> python create_matching.py \<optional_rl_config_path\>

\<optional_rl_config_path\> = path to record linkage configuration file, defaults to data/rl_config.json

### Evaluate Results

> python evaluator.py

To autimaticall conduct the three steps with a given exp_config.json file, run

> python launch_experiments.py \<path_to_exp_config.json\>

To automatically launch experiments with all relevant config files, call

> python main.py

# Streamlit

For visualization of results

## Start local server

> cd MainModule

> streamlit run streamlit_entry.py --server.fileWatcherType none

# Miscellaneous

## Freeze requirements when conda env activated

> pip list --format=freeze \> requirements.txt
