import subprocess
import time
import os
import signal

# Helper to run and stream logs in real time (non-blocking)
def run_background(cmd):
    print(f"\n🚀 Launching (background): {cmd}")
    return subprocess.Popen(cmd, shell=True, preexec_fn=os.setsid)

# Helper to run blocking commands
def run(cmd):
    print(f"\n🟢 Running: {cmd}")
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        print(f"❌ Command failed: {cmd}")
        exit(1)

def main():
    # Move to project root
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    os.chdir(project_root)

    # 1. Start Docker Compose and build images
    run("docker compose up --build -d")

    # 2. Wait a few seconds to ensure services start
    print("\n⏳ Waiting 15 seconds for containers to stabilize...")
    time.sleep(15)

    # 3. Create Kafka topics using topic-init container; then, stop the container in background
    run("docker exec topic-init python3 /app/create_topics.py")
    run_background("docker stop topic-init")

    # 4. Start batch producer in background
    run_background("docker exec batch-producer python3 /app/batch_producer.py")

    # 5. Start Flink job in background
    run_background("docker exec jobmanager flink run -py /app/jobs/l-pbf_job.py")
    
    # 6. Start csv writer in background
    run_background("docker exec csv-writer python3 /app/kafka_to_csv_stream_writer.py")

if __name__ == "__main__":
    main()

