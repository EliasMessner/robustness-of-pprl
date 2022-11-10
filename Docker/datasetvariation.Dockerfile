FROM python:3.9
RUN pip install --upgrade pip
ADD requirements.txt .
RUN pip install -r requirements.txt
COPY /MainModule ./MainModule
WORKDIR /MainModule/
CMD ["python", "-u", "dataset_variation.py"]