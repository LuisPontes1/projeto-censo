import pandas as pd

DIAMOND_FILE = r"c:\projetos\projeto-censo\data\diamond\censo_2022_diamond_features_final.parquet"

df = pd.read_parquet(DIAMOND_FILE)
cols = df.columns.tolist()

print("--- LISA Columns ---")
lisa_cols = [c for c in cols if 'lisa' in c.lower()]
print(lisa_cols[:20]) # Print first 20 to avoid spam

print("\n--- Income (Renda) Columns ---")
renda_cols = [c for c in cols if 'renda' in c.lower() or 'rendimento' in c.lower()]
print(renda_cols[:20])

print("\n--- Other Indices ---")
index_cols = [c for c in cols if 'index' in c.lower() and c not in lisa_cols]
print(index_cols[:20])
