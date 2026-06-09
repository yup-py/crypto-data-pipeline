import pandas as pd

# Read the local parquet file
df = pd.read_parquet('clean.parquet', engine='pyarrow')

# Inspect the transformed data
print(df.head())
print(df.info())