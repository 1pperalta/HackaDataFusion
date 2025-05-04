from src.pipeline.clustering.train import main

def run_pipeline():
    """
    Entry point for the clustering pipeline
    """
    print("Starting clustering pipeline with 15% data sample and KMeans only...")
    main()
    print("Pipeline completed!")

if __name__ == "__main__":
    run_pipeline()