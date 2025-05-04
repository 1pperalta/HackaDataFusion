import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
import os

def load_and_scale_data(path):
    if path.endswith('.parquet'):
        df = pd.read_parquet(path)
    elif path.endswith('.csv'):
        df = pd.read_csv(path)
    else:
        raise ValueError(f"Unsupported file format for {path}")

    df = df.sample(frac=0.15, random_state=42)
    df_numeric = df.select_dtypes(include=[np.number])
    if "repo_id" in df_numeric.columns:
        df_numeric = df_numeric.drop(columns=["repo_id"])

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(df_numeric)
    return X_scaled

def merge_and_scale_datasets(paths):
    dfs = []
    
    for path in paths:
        if not os.path.exists(path):
            print(f"Warning: File {path} does not exist. Skipping.")
            continue

        if path.endswith('.parquet'):
            df = pd.read_parquet(path)
        elif path.endswith('.csv'):
            df = pd.read_csv(path)
        else:
            print(f"Unsupported file format for {path}. Skipping.")
            continue

        if 'repo_id' in df.columns:
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            if not numeric_cols:
                print(f"No numeric columns in {path}. Skipping.")
                continue
            if df['repo_id'].duplicated().any():
                df = df.groupby('repo_id')[numeric_cols].mean().reset_index()
            dfs.append(df)
        else:
            print(f"No repo_id column in {path}. Skipping.")
    
    if not dfs:
        raise ValueError("No valid datasets to merge")
    
    merged_df = dfs[0]
    for i in range(1, len(dfs)):
        merged_df = pd.merge(merged_df, dfs[i], on='repo_id', how='inner')
    
    merged_df = merged_df.sample(frac=0.15, random_state=42)
    
    df_numeric = merged_df.select_dtypes(include=[np.number])
    numeric_columns = df_numeric.columns
    
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(df_numeric)
    
    return X_scaled, numeric_columns
