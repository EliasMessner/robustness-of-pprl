FROM python:3.10-alpine
COPY /MainModule ./MainModule
# COPY requirements.txt requirements.txt
# RUN pip install -r requirements.txt
WORKDIR /MainModule
CMD ["python3", "-u", "greet_client.py"]