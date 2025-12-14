import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import os

# Config
DATA_DIR = r'C:\projetos\projeto-censo\data'
DIAMOND_PATH = os.path.join(DATA_DIR, 'diamond', 'censo_2022_spatial_features.parquet')
SPATIAL_PATH = os.path.join(DATA_DIR, 'spatial', 'malha_setores_2022.parquet')
PLOT_DIR = r'C:\projetos\projeto-censo\plots'
MUNI_NAME = 'São José dos Campos'

os.makedirs(PLOT_DIR, exist_ok=True)

def main():
    print(f"--- Inspecionando Resultados para: {MUNI_NAME} ---")
    
    # 1. Carregar Features (Diamond)
    print("Carregando Features Diamond...")
    df_diamond = pd.read_parquet(DIAMOND_PATH)
    
    # 2. Carregar Malha (Apenas colunas necessárias para filtro e geometria)
    print("Carregando Malha Espacial...")
    gdf_spatial = gpd.read_parquet(SPATIAL_PATH)
    
    # 3. Filtrar Município
    print(f"Filtrando por {MUNI_NAME}...")
    gdf_muni = gdf_spatial[gdf_spatial['name_muni'] == MUNI_NAME].copy()
    
    # Garantir chave de join
    if pd.api.types.is_float_dtype(gdf_muni['code_tract']):
        gdf_muni['code_tract'] = gdf_muni['code_tract'].astype(int).astype(str)
    else:
        gdf_muni['code_tract'] = gdf_muni['code_tract'].astype(str)
        
    print(f"Setores encontrados: {len(gdf_muni)}")
    
    # 4. Join
    print("Realizando Join...")
    gdf_final = gdf_muni.merge(df_diamond, left_on='code_tract', right_on='code_tract', how='inner')
    print(f"Setores com features: {len(gdf_final)}")
    
    # 5. Plots
    features_to_plot = [
        ('spatial_smooth_income_k5', 'Renda Média (Suavizada K=5)', 'Viridis'),
        ('spatial_lag_income_ratio', 'Ratio Renda (Eu / Vizinhos)', 'RdBu'),
        ('expert_spatial_cluster_income', 'Clusters de Renda (LISA)', 'Set1'),
        ('spatial_smooth_mortality_k5', 'Mortalidade Jovem (Suavizada K=5)', 'Reds')
    ]
    
    for col, title, cmap in features_to_plot:
        if col not in gdf_final.columns:
            print(f"⚠️ Coluna {col} não encontrada.")
            continue
            
        print(f"Gerando plot: {title}...")
        fig, ax = plt.subplots(figsize=(12, 10))
        
        # Plot base
        gdf_final.plot(column=col, ax=ax, legend=True, cmap=cmap, 
                       legend_kwds={'shrink': 0.6})
        
        ax.set_title(f"{MUNI_NAME} - {title}", fontsize=15)
        ax.set_axis_off()
        
        output_file = os.path.join(PLOT_DIR, f"{MUNI_NAME}_{col}.jpg")
        plt.savefig(output_file, dpi=150, bbox_inches='tight')
        plt.close()
        print(f"✅ Salvo: {output_file}")

if __name__ == "__main__":
    main()
