# MainModule

Python 3.9 needed

### Install requirements

If using conda environment, first install pip in conda env using

> conda activate <some-python-3.9-env>

> conda install pip

Install requirements

> cd MainModule

> pip install -r requirements.txt

### Run

> python main.py


# RLModule

### Build jar
> cd RLModule

> mvn package

### Run
> java -jar target/RLModule.jar -d <path-to-dataset> -o <path-to-outfile> -c <path-to-config-file>