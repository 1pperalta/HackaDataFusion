from sklearn.cluster import KMeans
from .evaluate import evaluate_clustering

def run_kmeans(X, params):
    k = 5
    model = KMeans(n_clusters=k, random_state=42)
    labels = model.fit_predict(X)
    scores = evaluate_clustering(X, labels)
    return [{
        "model": "KMeans",
        "params": {"k": k},
        **scores
    }], model
