# RLModule

### Build jar
> cd RLModule

> mvn package

### Run
> java -jar target/RLModule.jar -d <path-to-dataset> -o <path-to-outfile> -c <path-to-config-file>

# MainModule

Python 3.9 needed

### Install requirements

If using conda environment, first install pip in conda env using

> conda activate <some-python-3.9-env>

> conda install pip

Install requirements

> cd MainModule

> pip install -r requirements.txt

### Create all Dataset Variations

> python dataset_modifier.py

### Run Experiments

> python main.py
