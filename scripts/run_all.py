#!/usr/bin/env python3
"""
Start the whole architecture:
  - create Kafka topic
  - launch Flink job
  - launch CSV-writer
  - launch Client
"""

import subprocess
import time
import os
import argparse
import requests
import json

JOB_NAME = "L-PBF Job"


def positive_int(value: str) -> int:
    ivalue = int(value)
    if ivalue <= 0:
        raise argparse.ArgumentTypeError(f"{value} is not a positive integer")
    return ivalue

def run_background(cmd):
    print(f"\n🚀 Launching (background): {cmd}")
    return subprocess.Popen(cmd, shell=True, preexec_fn=os.setsid)

def run(cmd):
    print(f"\n🟢 Running: {cmd}")
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        print(f"❌ Command failed: {cmd}")
        exit(1)

def get_job_id(job_name: str, sleep_s: float = 1.0) -> str:
    while True:
        try:
            jobs_json = subprocess.check_output(
                "curl -s localhost:8081/jobs/overview",
                shell=True,
                text=True,
            )
            for job in json.loads(jobs_json)["jobs"]:
                if job["name"] == job_name:          
                    if job["state"] == "RUNNING":    
                        return job["jid"]
        except Exception as e:
            print(f"⚠️  get_job_id(): error {e}, trying again…")
            pass
        time.sleep(sleep_s)

def wait_vertices_ready(host: str, port: int, job_id: str,
                        ok_state=("RUNNING"),
                        poll_int=2.0):
    """
    Waits for *all* vertices of job_id to be running.
    """
    base_url = f"http://{host}:{port}"

    while True:
        try:
            data = requests.get(f"{base_url}/jobs/{job_id}",
                                timeout=3).json()
            states = {v["name"]: v["status"] for v in data.get("vertices", [])}
            pending = {n: s for n, s in states.items() if s not in ok_state}

            if not pending:
                print("✅ All vertices are RUNNING")
                return
            else:
                print(f"⏳ Vertex not ready yet: {pending}")
        except Exception as e:
            print(f"⚠️  /vertices not ready yet: {e}")

        time.sleep(poll_int)

def main():
    parser = argparse.ArgumentParser(description="Launcher for L‑PBF pipeline")
    parser.add_argument("--limit", type=positive_int,
                        help="Maximum number of batches to process")
    args = parser.parse_args()

    limit           = args.limit

    # Move to project root
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    os.chdir(project_root)

    # 1. Create Kafka topics using topic-init container; then, stop the container
    run(f"docker exec topic-init python3 /app/create_topics.py")
    run("docker stop topic-init")

    # 2. Start Flink job
    flink_job_cmd = (
        "docker exec jobmanager "
        "flink run -py /app/jobs/l-pbf_job.py --detached"
    )
    run(flink_job_cmd)

    # 🔄 2.b  Wait until the job is completely ready
    job_id = get_job_id(JOB_NAME)
    wait_vertices_ready(host="localhost", port=8081, job_id=job_id)
    
    # 3. Start csv writer in background (disable it for performance evaluation)
    run_background("docker exec csv-writer python3 /app/kafka_to_csv_stream_writer.py")
    
    ### For metrics evaluation ###
    #run("docker stop csv-writer")
    ##############################
        
    time.sleep(5)    
        
    # 4. Start client in background (pass --limit if provided)
    client_cmd = "docker exec l-pbf-client python3 /app/l-pbf_client.py"
    if limit is not None:
        client_cmd += f" --limit {limit}"
    run(client_cmd)

if __name__ == "__main__":
    main()

