FROM python:3.10-slim

USER root

WORKDIR /app

COPY client/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY client/l-pbf_client.py /app/l-pbf_client.py

