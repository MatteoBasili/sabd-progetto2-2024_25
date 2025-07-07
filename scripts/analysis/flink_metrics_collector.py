#!/usr/bin/env python3
"""
Flink Metrics Collector

Collects selected Flink task/operator metrics at a fixed polling interval
and stores them in a CSV file.

Example
-------
python flink_metrics_collector.py \
       --job-id 1234567890abcdef \
       --host localhost --port 8081 \
       --interval 1 \
       --metrics numRecordsInPerSecond,numRecordsOutPerSecond,latency \
       --output metrics.csv \
       --duration 300

Requirements
------------
* Python 3.8+
* requests >= 2.0 (`pip install requests`)
"""

import argparse
import csv
import datetime as dt
import time
import sys
from typing import List, Dict

import requests


def get_json(url: str):
    """Helper to GET a JSON endpoint with basic error handling."""
    try:
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        print(f"[WARN] Could not retrieve {url}: {exc}", file=sys.stderr)
        return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect Flink metrics via REST API and save them to CSV.")
    parser.add_argument("--host", default="localhost", help="JobManager REST host (default: localhost)")
    parser.add_argument("--port", type=int, default=8081, help="REST port (default: 8081)")
    parser.add_argument("--job-id", required=True, help="Target Flink job ID")
    parser.add_argument("--interval", type=float, default=1.0, help="Polling interval in seconds (default: 1)")
    parser.add_argument(
        "--metrics",
        default="numRecordsInPerSecond,numRecordsOutPerSecond,latency",
        help="Comma‑separated list of metric IDs to collect (default: common throughput+latency)",
    )
    parser.add_argument("--output", default="metrics.csv", help="Output CSV file (default: metrics.csv)")
    parser.add_argument(
        "--duration",
        type=int,
        default=0,
        help="Total collection time in seconds (0 = run until Ctrl‑C, default: 0)",
    )
    return parser.parse_args()


def discover_vertices(base_url: str, job_id: str) -> Dict[str, str]:
    url = f"{base_url}/jobs/{job_id}"
    while True:
        data = get_json(url)
        if data and "vertices" in data:
            return {v["id"]: v["name"] for v in data["vertices"]}
        time.sleep(1)        

def collect_metrics(
    base_url: str,
    job_id: str,
    vertices: Dict[str, str],
    metrics: List[str],
    interval: float,
    csv_writer: csv.DictWriter,
    duration: int,
):
    metric_query = ",".join(metrics)
    end_at = time.time() + duration if duration > 0 else None

    try:
        while True:
            now_iso = dt.datetime.utcnow().isoformat(timespec="seconds")
            for v_id, v_name in vertices.items():
                url = f"{base_url}/jobs/{job_id}/{v_id}/metrics?get={metric_query}"
                values = {m["id"]: m.get("value") for m in (get_json(url) or [])}
                row = {"timestamp": now_iso, "vertex_id": v_id, "vertex_name": v_name}
                row.update({metric: values.get(metric, "") for metric in metrics})
                csv_writer.writerow(row)
            # Flush to disk so progress is visible if tailing the file
            csv_writer.flush()

            if end_at and time.time() >= end_at:
                print("[INFO] Reached specified duration. Exiting …")
                break
            time.sleep(interval)
    except KeyboardInterrupt:
        print("[INFO] Interrupted by user. Exiting …")


def main():
    args = parse_args()
    base_url = f"http://{args.host}:{args.port}"

    print(f"[INFO] Discovering vertices for job {args.job_id} …")
    vertices = discover_vertices(base_url, args.job_id)
    print(f"[INFO] Found {len(vertices)} vertices.")

    metrics = [m.strip() for m in args.metrics.split(",") if m.strip()]
    fieldnames = ["timestamp", "vertex_id", "vertex_name", *metrics]

    print(f"[INFO] Collecting metrics every {args.interval}s → {args.output}")
    if args.duration:
        print(f"[INFO] Will run for {args.duration} seconds.")

    with open(args.output, "w", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        collect_metrics(
            base_url=base_url,
            job_id=args.job_id,
            vertices=vertices,
            metrics=metrics,
            interval=args.interval,
            csv_writer=writer,
            duration=args.duration,
        )


if __name__ == "__main__":
    main()
