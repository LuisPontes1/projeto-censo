import pandas as pd
import geopandas as gpd
import os

DATA_DIR = r"c:\projetos\projeto-censo\data"
GOLD_PATH = os.path.join(DATA_DIR, 'gold', 'censo_2022_features_final.parquet')
SPATIAL_PATH = os.path.join(DATA_DIR, 'spatial', 'malha_setores_2022.parquet')

print("Loading Gold...")
df_gold = pd.read_parquet(GOLD_PATH, columns=['CD_SETOR'])
print("Gold CD_SETOR sample:", df_gold['CD_SETOR'].head().tolist())
print("Gold CD_SETOR dtype:", df_gold['CD_SETOR'].dtype)

print("Loading Spatial...")
gdf_spatial = pd.read_parquet(SPATIAL_PATH, columns=['code_tract'])
print("Spatial code_tract sample:", gdf_spatial['code_tract'].head().tolist())
print("Spatial code_tract dtype:", gdf_spatial['code_tract'].dtype)

# Check intersection
gold_ids = set(df_gold['CD_SETOR'].astype(str))
spatial_ids = set(gdf_spatial['code_tract'].astype(str))

intersection = gold_ids.intersection(spatial_ids)
print(f"Intersection size: {len(intersection)}")
print(f"Gold size: {len(gold_ids)}")
print(f"Spatial size: {len(spatial_ids)}")

if len(intersection) == 0:
    print("NO MATCHES FOUND!")
    print("Sample Gold ID:", list(gold_ids)[0])
    print("Sample Spatial ID:", list(spatial_ids)[0])
