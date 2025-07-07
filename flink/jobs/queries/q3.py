import json
import logging

import numpy as np
from sklearn.cluster import DBSCAN       
from pyflink.common import Row

from config import DBSCAN_EPS, DBSCAN_MIN_SAMPLES

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Q3")


def cluster_outliers_sklearn(points,
                             eps = DBSCAN_EPS,
                             min_samples = DBSCAN_MIN_SAMPLES):
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

    centroids = []
    for label in set(labels):
        if label == -1:            # noise
            continue
        cluster_pts = positions[labels == label]
        centroid = cluster_pts.mean(axis=0)
        centroids.append({
            "x": float(centroid[0]),
            "y": float(centroid[1]),
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
            int(data["batch_id"]),
            str(data["print_id"]),
            int(data["tile_id"]),
            int(data["saturated"]),
            centroids_json
        )

    except Exception as exc:
        logger.error(f"[Q3] process_json error: {exc}")
        return None

