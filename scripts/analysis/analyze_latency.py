import pandas as pd
import re

LOG_FILE = "latency-300.log"

# ------------------------------------------------------------
# 1. Parse: extract timestamp, query, batch_id, latency
# ------------------------------------------------------------
pat = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+) .*"
    r"METRICS\|(?P<query>Q\d)\|batch=(?P<batch>\d+)\|latency_ms=(?P<lat>[\d\.]+)"
)

rows = []
with open(LOG_FILE) as fh:
    for line in fh:
        m = pat.search(line)
        if m:
            rows.append(
                dict(
                    timestamp=pd.to_datetime(m["ts"], format="%Y-%m-%d %H:%M:%S,%f"),
                    query=m["query"],
                    batch=int(m["batch"]),
                    latency_ms=float(m["lat"]),
                )
            )

df_raw = pd.DataFrame(rows)

# ------------------------------------------------------------
# 2. Aggregate by (query, batch) -> sum latency
# ------------------------------------------------------------
df = (
    df_raw.groupby(["query", "batch"], as_index=False)
    .agg(latency_ms_tot=("latency_ms", "sum"), first_ts=("timestamp", "min"))
)

# ------------------------------------------------------------
# 3. Latency statistics for queries
# ------------------------------------------------------------
lat_stats = (
    df.groupby("query")["latency_ms_tot"]
    .agg(
        mean_latency="mean",
        max_latency="max",
        p50_latency=lambda s: s.quantile(0.5),
        p90_latency=lambda s: s.quantile(0.9),
        p99_latency=lambda s: s.quantile(0.99),
        count="count",
    )
    .round(6)
)

# ------------------------------------------------------------
# 4. Throughput (batch/s) per query
# ------------------------------------------------------------
throughputs = []
for q, grp in df.groupby("query"):
    if len(grp) < 2:
        thr = None
    else:
        delta_sec = (grp["first_ts"].max() - grp["first_ts"].min()).total_seconds()
        thr = len(grp) / delta_sec if delta_sec else None
    throughputs.append({"query": q, "throughput_batches_per_sec": thr})

thr_df = pd.DataFrame(throughputs).round(6)

# ------------------------------------------------------------
# 5. Write results to file
# ------------------------------------------------------------
with pd.ExcelWriter("performance_metrics-300b.xlsx") as xls:
    lat_stats.to_excel(xls, sheet_name="Latency")
    thr_df.to_excel(xls, sheet_name="Throughput", index=False)
    
