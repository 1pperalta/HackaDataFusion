import os
import mlflow
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score, calinski_harabasz_score, davies_bouldin_score
import os

# Get the absolute path to the data directory
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_DIR = os.path.join(BASE_DIR, "data", "processed")

# Rutas a los archivos .parquet del nivel Gold
DATA_PATHS = [
    os.path.join(DATA_DIR, "gold/repo_metrics/2025-05-04.repo_metrics.parquet"),
    os.path.join(DATA_DIR, "gold/actor_metrics/2025-05-04.actor_metrics.parquet"),
    os.path.join(DATA_DIR, "gold/event_metrics/2025-05-04.event_metrics.parquet"),
    os.path.join(DATA_DIR, "gold/event_type_metrics/2025-05-04.event_type_metrics.parquet"),
    os.path.join(DATA_DIR, "gold/geographical_activity/2025-05-04.geographical_activity.parquet"),
    os.path.join(DATA_DIR, "gold/org_metrics/2025-05-04.org_metrics.parquet"),
    os.path.join(DATA_DIR, "gold/daily_summary/2025-05-04.daily_summary.parquet"),
    os.path.join(DATA_DIR, "gold/time_based_activity/2025-05-04.time_based_activity.parquet")
]


# Parámetros para KMeans
KMEANS_PARAMS = {
    "k_values": list(range(2, 11))
}

# Evaluación del clustering
def evaluate_clustering(X, labels):
    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)

    if n_clusters > 1:
        return {
            "silhouette": silhouette_score(X, labels),
            "calinski": calinski_harabasz_score(X, labels),
            "davies": davies_bouldin_score(X, labels)
        }
    else:
        return {
            "silhouette": -1,
            "calinski": -1,
            "davies": float("inf")
        }

# Cargar y escalar datos
def load_and_scale_data(path):
    # Check file extension and load accordingly
    if path.endswith('.parquet'):
        df = pd.read_parquet(path)
    elif path.endswith('.csv'):
        df = pd.read_csv(path)
    else:
        raise ValueError(f"Unsupported file format for {path}")

    # Take only 15% of the data
    df = df.sample(frac=0.15, random_state=42)
    
    # Selecciona solo columnas numéricas, omitiendo ID si existe
    df_numeric = df.select_dtypes(include=[np.number])
    if "repo_id" in df_numeric.columns:
        df_numeric = df_numeric.drop(columns=["repo_id"])

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(df_numeric)
    return X_scaled

# Unir y escalar datasets
def merge_and_scale_datasets(paths):
    dfs = []
    
    # Load each dataset
    for path in paths:
        # Skip if file doesn't exist
        if not os.path.exists(path):
            print(f"Warning: File {path} does not exist. Skipping.")
            continue
            
        # Load data
        if path.endswith('.parquet'):
            df = pd.read_parquet(path)
        elif path.endswith('.csv'):
            df = pd.read_csv(path)
        else:
            print(f"Unsupported file format for {path}. Skipping.")
            continue
        
        # Ensure repo_id exists for joining
        if 'repo_id' in df.columns:
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            
            # Skip if there are no numeric columns
            if not numeric_cols:
                print(f"No numeric columns in {path}. Skipping.")
                continue
                
            # Handle duplicates by taking the mean for each repo_id
            if df['repo_id'].duplicated().any():
                df = df.groupby('repo_id')[numeric_cols].mean().reset_index()
            
            dfs.append(df)
        else:
            print(f"No repo_id column in {path}. Skipping.")
    
    # Return empty if no valid datasets
    if not dfs:
        raise ValueError("No valid datasets to merge")
    
    # Merge all datasets on repo_id
    merged_df = dfs[0]
    for i in range(1, len(dfs)):
        merged_df = pd.merge(merged_df, dfs[i], on='repo_id', how='inner')
    
    # Sample 15% of the data
    merged_df = merged_df.sample(frac=0.15, random_state=42)
    
    # Drop repo_id and other non-numeric columns
    df_numeric = merged_df.select_dtypes(include=[np.number])
    numeric_columns = df_numeric.columns
    
    # Scale the data
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(df_numeric)
    
    return X_scaled, numeric_columns

# KMeans clustering
def run_kmeans(X, params):
    k = 5  # Fixed value instead of using params["k_values"]
    model = KMeans(n_clusters=k, random_state=42)
    labels = model.fit_predict(X)
    scores = evaluate_clustering(X, labels)
    return [{
        "model": "KMeans",
        "params": {"k": k},
        **scores
    }], model  # Return both results and model instance

# Función principal de entrenamiento
def main():
    # Usar la nueva función merge_and_scale_datasets para combinar múltiples datasets
    X_scaled, numeric_columns = merge_and_scale_datasets([
        DATA_PATHS[0],  # repo_metrics
        DATA_PATHS[1],  # actor_metrics
        DATA_PATHS[2],  # event_metrics
        DATA_PATHS[4],  # geographical_activity
        DATA_PATHS[5]   # org_metrics
    ])

    # Usar solo el modelo KMeans y obtener la instancia del modelo
    results, kmeans_model = run_kmeans(X_scaled, KMEANS_PARAMS)
    
    # Obtener el mejor modelo (solo hay uno ahora)
    best = results[0]

    # Extraer los centroides y mapear a las características originales
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
    
    # Guardar los centroides a un archivo CSV para su análisis posterior
    centroids_file = "cluster_centroids.csv"
    centroids_df.to_csv(centroids_file)
    print(f"\nCentroids saved to {centroids_file}")

# Ejecutar el pipeline
if __name__ == "__main__":
    print("Starting clustering pipeline with 15% data sample and KMeans only...")
    main()
    print("Pipeline completed!")
