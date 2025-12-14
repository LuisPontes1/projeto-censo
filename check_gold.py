import pandas as pd
import geopandas as gpd

path = r'c:\projetos\projeto-censo\data\gold\censo_2022_features_final.parquet'
try:
    df = pd.read_parquet(path)
    print("Columns:", df.columns.tolist()[:10])
    if 'geometry' in df.columns:
        print("Geometry column found.")
    else:
        print("No geometry column.")
except Exception as e:
    print(e)
