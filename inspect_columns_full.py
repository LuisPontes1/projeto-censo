import pandas as pd
import os

DATA_DIR = r"c:\projetos\projeto-censo\data"
GOLD_PATH = os.path.join(DATA_DIR, 'gold', 'censo_2022_features_final.parquet')

print("Reading Gold columns...")
df = pd.read_parquet(GOLD_PATH)
cols = df.columns.tolist()

print("\n--- SEARCHING FOR MASS COLUMNS ---")
keywords = ['renda', 'rendimento', 'massa', 'salarial', 'domicilio', 'pessoa', 'populacao', 'total']
for k in keywords:
    matches = [c for c in cols if k in c.lower()]
    print(f"\nKeyword '{k}': {len(matches)} matches")
    if len(matches) < 20:
        for m in matches: print(f"  - {m}")
    else:
        print(f"  - (First 5) {matches[:5]}")

print("\n--- EXACT MATCH CHECK ---")
targets = [
    'massa_salarial_total', 
    'total_domicilios', 
    'populacao_total',
    'total_de_pessoas_v0001',
    'total_de_domicilios_particulares_dppo_dppv_dppuo_dpio_v0003'
]
for t in targets:
    if t in cols:
        print(f"[OK] Found: {t}")
    else:
        print(f"[MISSING] Not found: {t}")
