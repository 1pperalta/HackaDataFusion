import mlflow
from . import config, features, models

def main():
    X_scaled = features.load_and_scale_data(config.DATA_PATH)

    results = []
    results += models.run_kmeans(X_scaled, config.KMEANS_PARAMS)
    results += models.run_dbscan(X_scaled, config.DBSCAN_PARAMS)
    results += models.run_agglomerative(X_scaled, config.AGLO_PARAMS)

    best = sorted(results, key=lambda x: x["silhouette"], reverse=True)[0]

    with mlflow.start_run():
        mlflow.log_params(best["params"])
        mlflow.log_metrics({
            "silhouette": best["silhouette"],
            "calinski": best["calinski"],
            "davies": best["davies"]
        })
        mlflow.set_tag("model_type", best["model"])

    print("✔ Mejor modelo encontrado:")
    print(best)

if __name__ == "__main__":
    main()
