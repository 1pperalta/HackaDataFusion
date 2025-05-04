from sklearn.metrics import silhouette_score, calinski_harabasz_score, davies_bouldin_score

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
