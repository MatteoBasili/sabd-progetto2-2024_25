FROM python:3.10-slim

USER root

WORKDIR /app

COPY csv-writer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY csv-writer/kafka_to_csv_stream_writer.py /app/kafka_to_csv_stream_writer.py

RUN mkdir -p /data/output && chmod 777 /data/output
