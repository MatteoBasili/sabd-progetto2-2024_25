import subprocess
import time
import os

# Helper to run and stream logs in real time
def run_background(cmd):
    print(f"\n🚀 Launching (background): {cmd}")
    return subprocess.Popen(cmd, shell=True)

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
    run("docker-compose up --build -d")

    # 2. Wait a few seconds to ensure services start
    print("\n⏳ Waiting 15 seconds for containers to stabilize...")
    time.sleep(15)

    # 3. Start batch producer in background
    producer_proc = run_background("docker exec batch-producer python3 /app/batch_producer.py")

    # 4. Start Flink job (also non-blocking but will exit after job is submitted)
    run("docker exec jobmanager flink run -py /app/q1_job.py")

    # 5. Wait for batch producer to finish (if it ever does — it's streaming)
    try:
        producer_proc.wait()
    except KeyboardInterrupt:
        print("\n🛑 Stopping batch producer...")
        producer_proc.terminate()

if __name__ == "__main__":
    main()

