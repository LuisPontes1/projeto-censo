import os
import pandas as pd
import geopandas as gpd
import numpy as np
from libpysal.weights import KNN
from esda.moran import Moran_Local
import duckdb
import time

# --- CONFIGURAÇÃO ---
DATA_DIR = r'C:\projetos\projeto-censo\data'
GOLD_PATH = os.path.join(DATA_DIR, 'gold', 'censo_2022_features_final.parquet')
SPATIAL_PATH = os.path.join(DATA_DIR, 'spatial', 'malha_setores_2022.parquet')
OUTPUT_DIR = os.path.join(DATA_DIR, 'diamond')
OUTPUT_PATH = os.path.join(OUTPUT_DIR, 'censo_2022_spatial_features.parquet')

os.makedirs(OUTPUT_DIR, exist_ok=True)

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")

def main():
    log("--- INICIANDO PROCESSAMENTO ESPACIAL (DIAMOND LAYER) ---")

    # 1. CARREGAR DADOS
    log("Carregando dados Gold (Tabular)...")
    df_gold = pd.read_parquet(GOLD_PATH)
    log(f"Gold carregado: {df_gold.shape}")

    log("Carregando Malha Espacial (Geometria)...")
    gdf_spatial = gpd.read_parquet(SPATIAL_PATH)
    # Garantir que code_tract é string para o join (Remover .0 se for float)
    if pd.api.types.is_float_dtype(gdf_spatial['code_tract']):
        gdf_spatial['code_tract'] = gdf_spatial['code_tract'].astype(np.int64).astype(str)
    else:
        gdf_spatial['code_tract'] = gdf_spatial['code_tract'].astype(str)
        
    log(f"Malha carregada: {gdf_spatial.shape}")

    # 2. JOIN (Tabular + Espacial)
    log("Realizando Join Espacial...")
    # Filtrar colunas da malha para economizar memória
    cols_spatial = ['code_tract', 'geometry', 'name_neighborhood', 'name_muni', 'name_state']
    gdf = gdf_spatial[cols_spatial].merge(df_gold, left_on='code_tract', right_on='CD_SETOR', how='inner')
    
    # Limpar memória
    del df_gold
    del gdf_spatial
    
    log(f"Dataset Unificado: {gdf.shape}")
    
    # Converter para CRS projetado (SIRGAS 2000 / Brazil Polyconic) para cálculos de distância precisos se necessário
    # Mas para KNN, lat/lon funciona ok se a escala for pequena, ou podemos projetar.
    # Vamos manter o original por enquanto para performance, o KNN do libpysal lida bem.

    # 3. DEFINIR VARIÁVEIS ALVO PARA SUAVIZAÇÃO
    # Mapeamento: Nome da Feature -> Nome da Coluna Original
    targets = {
        'income': 'renda_per_capita_sm', # Corrected from calc_income_per_capita_sm
        'mortality': 'expert_health_youth_mortality_rate',
        'literacy': 'expert_edu_literacy_rate_15_plus',
        'sanitation': 'expert_child_vuln_water_inadequate_pct', 
        'poverty': 'expert_family_single_parent_proxy' 
    }

    # Preencher NAs com 0 ou média para não quebrar o KNN
    for key, col in targets.items():
        if col in gdf.columns:
            gdf[col] = gdf[col].fillna(0)
        else:
            log(f"⚠️ Coluna {col} não encontrada! Pulando...")

    # 4. CONSTRUIR MATRIZ DE PESOS (KNN)
    # Vamos usar K=5 para "Micro-vizinhança"
    log("Calculando Matriz de Pesos KNN (K=5)... isso pode demorar um pouco...")
    # Usando os centróides para performance (muito mais rápido que polígonos)
    centroids = gdf.geometry.centroid
    w_k5 = KNN.from_dataframe(gdf, k=5, geom_col='geometry') # Libpysal usa centroids automaticamente se passar poligono, ou podemos passar centroids
    w_k5.transform = 'r' # Row-standardized (média)
    
    log("Matriz KNN-5 calculada.")

    # 5. CAMADA 1: CONTEXTO (SMOOTHING)
    log("Gerando Features de Contexto (Smoothing)...")
    
    for name, col in targets.items():
        if col not in gdf.columns: continue
        
        # Lag Espacial (Média dos vizinhos)
        # pysal.lag_spatial calcula a média dos vizinhos (se w for row-standardized)
        import libpysal
        
        # Feature: Smooth (Média Vizinhos)
        smooth_col = f'spatial_smooth_{name}_k5'
        gdf[smooth_col] = libpysal.weights.lag_spatial(w_k5, gdf[col])
        log(f"  -> Criada: {smooth_col}")

    # 6. CAMADA 2: FRICÇÃO & SEGREGAÇÃO (LAGS)
    log("Gerando Features de Fricção (Lags)...")
    
    for name, col in targets.items():
        if col not in gdf.columns: continue
        
        smooth_col = f'spatial_smooth_{name}_k5'
        
        # Feature: Ratio (Eu / Vizinhos)
        # Adiciona 0.001 para evitar divisão por zero
        ratio_col = f'spatial_lag_{name}_ratio'
        gdf[ratio_col] = (gdf[col] + 0.001) / (gdf[smooth_col] + 0.001)
        log(f"  -> Criada: {ratio_col}")

    # 7. CAMADA 3: CLUSTERS (LISA - Local Moran's I)
    log("Gerando Clusters LISA (Hotspots)...")
    
    # Vamos calcular apenas para Renda e Mortalidade (as mais críticas)
    lisa_targets = ['income', 'mortality']
    
    for name in lisa_targets:
        col = targets[name]
        if col not in gdf.columns: continue
        
        log(f"  Calculando LISA para {name}...")
        # Moran Local
        moran = Moran_Local(gdf[col], w_k5)
        
        # Feature: Cluster Category (q)
        # 1=HH, 2=LH, 3=LL, 4=HL
        cluster_col = f'expert_spatial_cluster_{name}'
        
        # Filtrar apenas significativos (p-value < 0.05)
        # Se não for significativo, vira 0 (NS)
        sig = moran.p_sim < 0.05
        clusters = moran.q * sig
        
        gdf[cluster_col] = clusters
        log(f"  -> Criada: {cluster_col} (0=NS, 1=HH, 2=LH, 3=LL, 4=HL)")

    # 8. SALVAR
    log("Salvando camada Diamond...")
    
    # Remover geometria para salvar em Parquet puro (mais leve para leitura tabular)
    # Ou manter se quisermos usar no QGIS. Vamos manter geometria por enquanto, 
    # mas idealmente teríamos dois arquivos.
    # Para o modelo de crédito, não precisamos da geometria, só do CD_SETOR e as features.
    
    # Vamos salvar apenas as colunas novas + chaves
    cols_to_keep = ['CD_SETOR', 'code_tract'] + \
                   [c for c in gdf.columns if 'spatial_' in c or 'expert_spatial_' in c]
    
    df_final = pd.DataFrame(gdf[cols_to_keep]) # Drop geometry conversion
    
    df_final.to_parquet(OUTPUT_PATH)
    log(f"🎉 SUCESSO! Arquivo salvo em: {OUTPUT_PATH}")
    log(f"Total de Features Espaciais: {len(cols_to_keep) - 2}")

if __name__ == "__main__":
    main()
