FROM python:3.10-slim

## 🧩 Install Java (for PyFlink)
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# 🔍 Set correct JAVA_HOME
RUN ln -s "$(dirname $(readlink -f $(which java)))/.." /opt/java
ENV JAVA_HOME=/opt/java
ENV PATH="$JAVA_HOME/bin:$PATH"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY q1_job.py .
COPY tiff_client.py .

CMD ["python", "q1_job.py"]

