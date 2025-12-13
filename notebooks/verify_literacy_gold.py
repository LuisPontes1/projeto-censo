import duckdb
import pandas as pd
import os

parquet_path = r'C:\projetos\projeto-censo\data\gold\censo_2022_features_final.parquet'

print(f"Verificando arquivo: {parquet_path}")

if not os.path.exists(parquet_path):
    print("Arquivo não encontrado!")
    exit()

con = duckdb.connect()
df = con.execute(f"SELECT * FROM read_parquet('{parquet_path}') LIMIT 10").df()

print("\nColunas disponíveis:")
print(df.columns.tolist())

if 'taxa_alfabetizacao_15_mais' in df.columns:
    print("\n--- Amostra de Taxa de Alfabetização ---")
    print(df[['CD_SETOR', 'taxa_alfabetizacao_15_mais']].head())
    
    # Check for nulls or weird values
    stats = con.execute(f"""
        SELECT 
            MIN(taxa_alfabetizacao_15_mais) as min_rate,
            MAX(taxa_alfabetizacao_15_mais) as max_rate,
            AVG(taxa_alfabetizacao_15_mais) as avg_rate,
            COUNT(*) as total_rows,
            COUNT(taxa_alfabetizacao_15_mais) as non_null_rows
        FROM read_parquet('{parquet_path}')
    """).df()
    print("\n--- Estatísticas ---")
    print(stats)
else:
    print("\nERRO: Coluna 'taxa_alfabetizacao_15_mais' NÃO encontrada!")
