import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
import os

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

def merge_and_scale_datasets(paths):
    """
    Merge multiple datasets based on repo_id and scale the numeric features
    
    Args:
        paths: List of file paths to the datasets
        
    Returns:
        X_scaled: Scaled numeric features
        numeric_columns: Names of the numeric columns
    """
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
            # Keep only the repo_id and numeric columns
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
