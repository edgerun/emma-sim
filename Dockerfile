FROM python:3-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY simulation/ .
RUN python setup.py install

WORKDIR /data

ENTRYPOINT [ "python", "-m", "simulation.emma" ]
