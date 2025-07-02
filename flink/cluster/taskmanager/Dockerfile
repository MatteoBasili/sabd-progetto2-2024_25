FROM apache/flink:2.0.0-scala_2.12

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
    
WORKDIR /app

COPY flink/jobs /app/jobs
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir -r /app/jobs/requirements.txt
    
# Download flink-python JARs
RUN wget https://repo1.maven.org/maven2/org/apache/flink/flink-python/2.0.0/flink-python-2.0.0.jar -P /opt/flink/lib/ && \
    wget https://repo1.maven.org/maven2/org/apache/flink/flink-streaming-python_2.12/1.8.3/flink-streaming-python_2.12-1.8.3.jar -P /opt/flink/lib/

# Kafka plugin
COPY flink/cluster/plugins/kafka /opt/flink/plugins/kafka

ENV PYTHONPATH=/app/jobs

COPY flink/cluster/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["taskmanager"]

