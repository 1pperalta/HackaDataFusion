from sklearn.cluster import KMeans, DBSCAN, AgglomerativeClustering
from .evaluate import evaluate_clustering

def run_kmeans(X, params):
    results = []
    for k in params["k_values"]:
        model = KMeans(n_clusters=k, random_state=42)
        labels = model.fit_predict(X)
        scores = evaluate_clustering(X, labels)
        results.append({
            "model": "KMeans",
            "params": {"k": k},
            **scores
        })
    return results

def run_dbscan(X, params):
    results = []
    for eps in params["eps_values"]:
        for min_samples in params["min_samples_values"]:
            model = DBSCAN(eps=eps, min_samples=min_samples)
            labels = model.fit_predict(X)
            scores = evaluate_clustering(X, labels)
            results.append({
                "model": "DBSCAN",
                "params": {"eps": eps, "min_samples": min_samples},
                **scores
            })
    return results

def run_agglomerative(X, params):
    results = []
    for k in params["k_values"]:
        model = AgglomerativeClustering(n_clusters=k)
        labels = model.fit_predict(X)
        scores = evaluate_clustering(X, labels)
        results.append({
            "model": "Agglomerative",
            "params": {"k": k},
            **scores
        })
    return results
