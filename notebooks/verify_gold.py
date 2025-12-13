import duckdb
import os

output_parquet = r'C:\projetos\projeto-censo\data\gold\censo_2022_features_final.parquet'

con = duckdb.connect(':memory:')
if os.path.exists(output_parquet):
    print(f"Inspecting Final Dataset: {output_parquet}")
    # Get columns
    df_cols = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{output_parquet}')").df()
    cols = df_cols['column_name'].tolist()
    
    print(f"Total Columns: {len(cols)}")
    
    # Check for specific expert features
    expert_features = [
        'taxa_mortalidade_1000', 
        'taxa_conjugalidade', 
        'taxa_monoparental_proxy', 
        'taxa_multigeracional',
        'pct_domicilios_unipessoais',
        'indice_potencial_consumo',
        'indice_diversidade_racial'
    ]
    
    print("\n--- Checking Expert Features ---")
    for feat in expert_features:
        if feat in cols:
            print(f"[OK] {feat}")
            # Show some stats
            stats = con.execute(f"SELECT MIN({feat}), AVG({feat}), MAX({feat}) FROM read_parquet('{output_parquet}')").fetchone()
            print(f"    Min: {stats[0]:.4f}, Avg: {stats[1]:.4f}, Max: {stats[2]:.4f}")
        else:
            print(f"[MISSING] {feat}")

else:
    print("File not found.")
