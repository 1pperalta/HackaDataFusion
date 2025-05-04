import os

# Get the absolute path to the data directory
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
DATA_DIR = os.path.join(BASE_DIR, "data", "processed")

# Rutas a los archivos .csv del nivel Gold
DATA_PATHS = [
    os.path.join(DATA_DIR, "gold/repo_metrics/2025-05-04.repo_metrics.csv"),
    os.path.join(DATA_DIR, "gold/actor_metrics/2025-05-04.actor_metrics.csv"),
    os.path.join(DATA_DIR, "gold/event_metrics/2025-05-04.event_metrics.csv"),
    os.path.join(DATA_DIR, "gold/event_type_metrics/2025-05-04.event_type_metrics.csv"),
    os.path.join(DATA_DIR, "gold/geographical_activity/2025-05-04.geographical_activity.csv"),
    os.path.join(DATA_DIR, "gold/org_metrics/2025-05-04.org_metrics.csv"),
    os.path.join(DATA_DIR, "gold/daily_summary/2025-05-04.daily_summary.csv"),
    os.path.join(DATA_DIR, "gold/time_based_activity/2025-05-04.time_based_activity.csv")
]

# Parámetros para KMeans
KMEANS_PARAMS = {
    "k_values": list(range(2, 11))
}

# Parámetros para DBSCAN
DBSCAN_PARAMS = {
    "eps_values": [0.3, 0.5, 0.7, 1.0],
    "min_samples_values": [3, 5, 10]
}

# Parámetros para Agglomerative Clustering
AGGLO_PARAMS = {
    "k_values": list(range(2, 11))
}
