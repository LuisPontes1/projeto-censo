import os
import geobr
import geopandas as gpd

def download_census_mesh(year=2022):
    output_dir = r'C:\projetos\projeto-censo\data\spatial'
    os.makedirs(output_dir, exist_ok=True)
    
    output_path = os.path.join(output_dir, f'malha_setores_{year}.parquet')
    
    if os.path.exists(output_path):
        print(f"✅ Malha de setores {year} já existe em: {output_path}")
        return

    print(f"⬇️ Baixando Malha de Setores Censitários do Brasil ({year})...")
    print("Isso pode demorar alguns minutos (arquivo grande)...")
    
    try:
        # Download data for the whole country
        # geobr caches data, so subsequent runs are faster
        gdf = geobr.read_census_tract(code_tract="all", year=year)
        
        print(f"✅ Download concluído. Convertendo para Parquet...")
        
        # Save as Parquet for performance
        gdf.to_parquet(output_path)
        
        print(f"🎉 Sucesso! Arquivo salvo em: {output_path}")
        print(f"Total de setores: {len(gdf)}")
        print(f"Colunas: {list(gdf.columns)}")
        
    except Exception as e:
        print(f"❌ Erro ao baixar malha: {e}")

if __name__ == "__main__":
    download_census_mesh()
