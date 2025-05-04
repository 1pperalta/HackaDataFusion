import mlflow
import pandas as pd
import numpy as np
from src.data_flow.models import config, features, models

def main():
    X_scaled, numeric_columns = features.merge_and_scale_datasets([
        config.DATA_PATHS[0], 
        config.DATA_PATHS[1], 
        config.DATA_PATHS[2], 
        config.DATA_PATHS[4], 
        config.DATA_PATHS[5]
    ])

    results, kmeans_model = models.run_kmeans(X_scaled, config.KMEANS_PARAMS)
    best = results[0]

    centroids = kmeans_model.cluster_centers_
    centroids_df = pd.DataFrame(centroids, columns=numeric_columns)
    centroids_df.index.name = 'Cluster'

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
    
    print("\nCentroids:")
    print(centroids_df)
    
    centroids_file = "cluster_centroids.csv"
    centroids_df.to_csv(centroids_file)
    print(f"\nCentroids saved to {centroids_file}")

if __name__ == "__main__":
    main()
