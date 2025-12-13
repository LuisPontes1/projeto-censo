import duckdb
import os

data_dir = r'C:\projetos\projeto-censo\data\silver'
obitos_path = os.path.join(data_dir, 'Agregados_por_setores_obitos_BR.parquet')

con = duckdb.connect(':memory:')
if os.path.exists(obitos_path):
    print(f"Inspecting: {obitos_path}")
    df = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{obitos_path}')").df()
    print(df[['column_name', 'column_type']].to_string())
else:
    print("File not found.")
