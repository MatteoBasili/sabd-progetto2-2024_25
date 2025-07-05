import json
import logging
from typing import List, Tuple, Dict

import numpy as np
from sklearn.cluster import DBSCAN       
from pyflink.common import Row

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Q3")

# --------------------------- Params ----------------------------
EPS = 20          # radius (pixels)
MIN_SAMPLES = 5   # minimum points per cluster
# ---------------------------------------------------------------


def cluster_outliers_sklearn(points: List[Tuple[int, int]],
                             eps: float = EPS,
                             min_samples: int = MIN_SAMPLES) -> List[Dict]:
    """
    Cluster with DBSCAN (scikit-learn) and return the list of centroids
    in the requested format [{x, y, count}, …]. Noise points (label=‑1)
    are NOT included.
    """
    if not points:
        return []

    positions = np.asarray(points, dtype=np.float64)
    dbs = DBSCAN(eps=eps, min_samples=min_samples, metric="euclidean").fit(positions)
    labels = dbs.labels_

    centroids: List[Dict] = []
    for label in set(labels):
        if label == -1:            # noise
            continue
        cluster_pts = positions[labels == label]
        centroid = cluster_pts.mean(axis=0)
        centroids.append({
            "x": round(float(centroid[0]), 6),
            "y": round(float(centroid[1]), 6),
            "count": int(cluster_pts.shape[0])
        })
    return centroids


def process_json(raw_json: str) -> Row | None:
    """
    Convert JSON from Q2 → Row(Flink) ready for CSV:
        batch_id, print_id, tile_id, saturated, centroids(json‑compact)
    """
    try:
        data = json.loads(raw_json)
        required = {"batch_id", "print_id", "tile_id", "saturated", "outliers"}
        if not required.issubset(data):
            logger.warning(f"[Q3] missing fields: {data}")
            return None

        points = [(o["x"], o["y"]) for o in data["outliers"]]
        centroids = cluster_outliers_sklearn(points)

        centroids_json = json.dumps(centroids, separators=(",", ":"))

        return Row(
            batch_id=int(data["batch_id"]),
            print_id=str(data["print_id"]),
            tile_id=int(data["tile_id"]),
            saturated=int(data["saturated"]),
            centroids=centroids_json
        )

    except Exception as exc:
        logger.error(f"[Q3] process_json error: {exc}")
        return None

