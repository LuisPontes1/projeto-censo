import geopandas as gpd
gdf = gpd.read_parquet(r"c:\projetos\projeto-censo\data\spatial\malha_setores_2022.parquet")
print(gdf.columns.tolist())
