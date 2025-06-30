FROM python:3.10-slim

USER root

WORKDIR /app

COPY kafka/init/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY kafka/init/create_topics.py .

