import duckdb
import os

path = r'C:\projetos\projeto-censo\data\silver\Agregados_por_setores_entorno_domicilios_BR.parquet'

con = duckdb.connect(':memory:')
if os.path.exists(path):
    print(f"Inspecting: {path}")
    df = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").df()
    print(df[['column_name', 'column_type']].head(20).to_string())
