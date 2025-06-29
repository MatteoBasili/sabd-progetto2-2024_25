FROM python:3.10-slim

# 🧩 Install Java for PyFlink
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# 🔍 Set JAVA_HOME
RUN ln -s "$(dirname $(readlink -f $(which java)))/.." /opt/java
ENV JAVA_HOME=/opt/java
ENV PATH="$JAVA_HOME/bin:$PATH"

# ⚙️ Set working dir
WORKDIR /app

# 🔽 Install Python deps
COPY flink/q2-job/requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 📦 Copy app code
COPY flink/q2-job/q2_job.py .

# 📦 Copy Flink Kafka connector jars
COPY flink/plugins/kafka /opt/flink/plugins/kafka

# 🔁 Set environment variable if needed for Flink plugin path
ENV FLINK_PLUGINS_DIR=/opt/flink/plugins

# 🚀 Run job
CMD ["python3", "q2_job.py"]

