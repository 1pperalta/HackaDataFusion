import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler

def load_and_scale_data(path):
    df = pd.read_parquet(path)

    # Selecciona solo columnas numéricas, omitiendo ID si existe
    df_numeric = df.select_dtypes(include=[np.number])
    if "repo_id" in df_numeric.columns:
        df_numeric = df_numeric.drop(columns=["repo_id"])

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(df_numeric)
    return X_scaled
