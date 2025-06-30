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

    # 3. Create Kafka topics using topic-init container
    run("docker exec topic-init python3 /app/create_topics.py")

    # 4. Start batch producer in background
    producer_proc = run_background("docker exec batch-producer python3 /app/batch_producer.py")

    # 5. Start Flink job Q1 in background
    q1_proc = run_background("docker exec jobmanager flink run -py /app/q1_job.py")

    # 6. Start Flink job Q2 in background
    #q2_proc = run_background("docker exec jobmanager flink run -py /app/q2_job.py")

    # 7. Wait for batch producer to finish (likely runs indefinitely)
    try:
        producer_proc.wait()
    except KeyboardInterrupt:
        print("\n🛑 Stopping batch producer and Flink jobs...")

        # Terminate batch producer
        producer_proc.terminate()
        try:
            producer_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            producer_proc.kill()

        # Terminate flink jobs
        for proc in [q1_proc]:
        #for proc in [q1_proc, q2_proc]:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)

if __name__ == "__main__":
    main()

