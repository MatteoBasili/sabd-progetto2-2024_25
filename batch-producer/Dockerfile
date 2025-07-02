FROM python:3.10-slim

USER root

WORKDIR /app

COPY batch-producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY batch-producer/batch_producer.py /app/batch_producer.py

