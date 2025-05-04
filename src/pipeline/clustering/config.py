# Configuración de rutas y parámetros para los modelos

DATA_PATH = "data/gold/repo_metrics/repo_activity_metrics.parquet"

KMEANS_PARAMS = {
    "k_values": list(range(2, 11))
}

DBSCAN_PARAMS = {
    "eps_values": [0.3, 0.5, 0.7, 1.0],
    "min_samples_values": [3, 5, 10]
}

AGGLO_PARAMS = {
    "k_values": list(range(2, 11))
}
