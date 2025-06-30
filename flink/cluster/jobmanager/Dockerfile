FROM apache/flink:2.0.0-scala_2.12

# Install Python3 + pip
USER root
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set working dir
WORKDIR /app

# Copy requirements and install python deps (pyflink, requests, ecc)
COPY flink/jobs/requirements.txt .
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir -r requirements.txt
    
# Download flink-python JARs
RUN wget https://repo1.maven.org/maven2/org/apache/flink/flink-python/2.0.0/flink-python-2.0.0.jar -P /opt/flink/lib/ && \
    wget https://repo1.maven.org/maven2/org/apache/flink/flink-streaming-python_2.12/1.8.3/flink-streaming-python_2.12-1.8.3.jar -P /opt/flink/lib/

COPY flink/jobs/q1_job.py /app/q1_job.py
COPY flink/jobs/q2_job.py /app/q2_job.py

# Copy Kafka plugin
COPY flink/cluster/plugins/kafka /opt/flink/plugins/kafka

COPY flink/cluster/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["jobmanager"]

