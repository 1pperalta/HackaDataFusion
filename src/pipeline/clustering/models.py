from sklearn.cluster import KMeans
from .evaluate import evaluate_clustering

def run_kmeans(X, params):
    # Use a fixed number of clusters instead of grid search
    k = 5  # Fixed value instead of using params["k_values"]
    model = KMeans(n_clusters=k, random_state=42)
    labels = model.fit_predict(X)
    scores = evaluate_clustering(X, labels)
    return [{
        "model": "KMeans",
        "params": {"k": k},
        **scores
    }], model  # Return both results and model instance
