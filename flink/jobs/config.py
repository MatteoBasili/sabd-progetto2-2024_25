"""
Centralized configuration parameters.
"""

# Q1 - threshold-based analysis (16-it value)
EMPTY_THRESHOLD      = 5_000
SATURATION_THRESHOLD = 65_000          

# Q2 ― 3-D outlier search
DISTANCE_FACTOR      = 2               # “Manhattan” radius
OUTLIER_THRESHOLD    = 6_000

# Q3 ― DBSCAN clustering
DBSCAN_EPS           = 20              # pixels
DBSCAN_MIN_SAMPLES   = 5               # minimum points per cluster

