import duckdb
import os
import pandas as pd

parquet_path = r'C:\projetos\projeto-censo\data\silver\Agregados_por_setores_caracteristicas_domicilio3_BR_20250417.parquet'

if os.path.exists(parquet_path):
    con = duckdb.connect()
    df = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')").df()
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.max_rows', None)
    print(df['column_name'])
else:
    print("Arquivo não encontrado.")
