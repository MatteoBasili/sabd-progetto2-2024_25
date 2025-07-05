#!/usr/bin/env python3
"""
Start the whole architecture:
  - docker compose up
  - create Kafka topic
  - launch Flink job, CSV-writer and Client
  
If --limit is given you MUST also provide --tiles-per-layer,
because the client needs both to compute the expected number of results.
"""

import subprocess
import time
import os
import argparse
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("launcher")

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

def main():
    parser = argparse.ArgumentParser(description="Launcher for L‑PBF pipeline")
    parser.add_argument("--limit", type=positive_int,
                        help="Maximum number of batches to process")
    parser.add_argument("--tiles-per-layer", type=positive_int,
                        help="Tiles composing ONE layer (required if --limit is set)")
    args = parser.parse_args()

    limit           = args.limit
    tiles_per_layer = args.tiles_per_layer

    # --- arg‑check ----------------------------------------------------
    if limit is not None and tiles_per_layer is None:
        parser.error("--tiles-per-layer is required when --limit is set")
    # -----------------------------------------------------------------
    if limit is not None:
        expected_results = limit - (2 * tiles_per_layer)
        if expected_results <= 0:
            log.error(
                "Configuration error: limit=%s, tiles_per_layer=%s ⇒ "
                "expected_results = %s ≤ 0\n"
                "Increase --limit so that it is > 2 * tiles_per_layer (>%s).",
                limit, tiles_per_layer, expected_results, 2 * tiles_per_layer
            )
            exit(1)
    # -----------------------------------------------------------------

    # Move to project root
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    os.chdir(project_root)

    # 1. Start Docker Compose and build images
    run("docker compose up --build -d")

    # 2. Wait a few seconds to ensure services start
    print("\n⏳ Waiting 15 seconds for containers to stabilize...")
    time.sleep(15)

    # 3. Create Kafka topics using topic-init container; then, stop the container
    run("docker exec topic-init python3 /app/create_topics.py")
    run_background("docker stop topic-init")

    # 4. Start Flink job (blocking until REST reports it as RUNNING)
    flink_job_cmd = (
        "docker exec jobmanager "
        "flink run -py /app/jobs/l-pbf_job.py --detached"
    )
    run(flink_job_cmd)

    # 🔄 4.b  Wait until the job appears in Flink REST
    print("\n⏳ Waiting for Flink job to reach RUNNING state...")
    while True:
        try:
            out = subprocess.check_output(
                "curl -s jobmanager:8081/jobs/overview | jq '.jobs[0].state'",
                shell=True,
                text=True
            ).strip('" \n')
            if out == "RUNNING":
                print("✅ Flink job is RUNNING")
                break
        except Exception:
            pass
        time.sleep(1)
    else:
        print("⚠️  Job not RUNNING yet...")
    
    # 5. Start csv writer in background
    run_background("docker exec csv-writer python3 /app/kafka_to_csv_stream_writer.py")
        
    time.sleep(5)    
        
    # 6. Start client in background (pass --limit if provided)
    client_cmd = "docker exec l-pbf-client python3 /app/l-pbf_client.py"
    if limit is not None and limit > 0:
        client_cmd += f" --limit {limit} --tiles-per-layer {tiles_per_layer}"
    run_background(client_cmd)

if __name__ == "__main__":
    main()

