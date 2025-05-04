import mlflow
import pandas as pd
import numpy as np
from src.pipeline.clustering import config, features, models

def main():
    # Use the new merge_and_scale_datasets function to combine multiple datasets
    X_scaled, numeric_columns = features.merge_and_scale_datasets([
        config.DATA_PATHS[0],  # repo_metrics
        config.DATA_PATHS[1],  # actor_metrics
        config.DATA_PATHS[2],  # event_metrics
        config.DATA_PATHS[4],  # geographical_activity
        config.DATA_PATHS[5]   # org_metrics
    ])

    # Only use KMeans model and get the model instance back
    results, kmeans_model = models.run_kmeans(X_scaled, config.KMEANS_PARAMS)
    
    # Get the best model (only one model now)
    best = results[0]

    # Extract centroids and map to original features
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
    
    # Save centroids to CSV for further analysis
    centroids_file = "cluster_centroids.csv"
    centroids_df.to_csv(centroids_file)
    print(f"\nCentroids saved to {centroids_file}")

if __name__ == "__main__":
    main()
