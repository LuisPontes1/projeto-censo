import os
import time
import warnings
import gc
import numpy as np
import pandas as pd
import geopandas as gpd
from scipy.spatial import cKDTree
from joblib import Parallel, delayed
from libpysal.weights import KNN
from esda.moran import Moran_Local

# ==============================================================================
# CONFIGURATION
# ==============================================================================
# Adjust paths as necessary for your environment
# Robust path resolution based on script location
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')

GOLD_PATH = os.path.join(DATA_DIR, 'gold', 'censo_2022_features_final.parquet')
SPATIAL_PATH = os.path.join(DATA_DIR, 'spatial', 'malha_setores_2022.parquet')
OUTPUT_FILE = os.path.join(DATA_DIR, 'diamond', 'censo_2022_diamond_features.parquet')

# CRS Configuration
# EPSG:4674 is SIRGAS 2000 (Lat/Lon)
# EPSG:5880 is SIRGAS 2000 / Brazil Polyconic (Projected, Meters)
# CRITICAL: Must use a metric CRS for accurate distance/centroid calculations
TARGET_CRS = "EPSG:5880"

# Parallelization Settings
# n_jobs=-1 uses all available cores. 
# If memory is an issue, reduce this number (e.g., 4 or 8).
N_JOBS = 4 
BACKEND = 'loky'

# Feature Configuration
KNN_KS = [5, 10, 15]

# ==============================================================================
# WORKER FUNCTION - GRAVITY
# ==============================================================================
def calculate_gravity_chunk(indices_chunk, coords, mass_values, beta):
    """
    Calculates gravity values for a chunk of indices.
    """
    import numpy as np
    
    results = np.zeros(len(indices_chunk))
    
    for i, (idx_original, neighbors) in enumerate(indices_chunk):
        if not neighbors: continue
        
        center_pt = coords[idx_original]
        neigh_pts = coords[neighbors]
        
        dists = np.linalg.norm(neigh_pts - center_pt, axis=1)
        dists = np.maximum(dists, 1.0)
        
        masses = mass_values[neighbors]
        results[i] = np.sum(masses / (dists ** beta))
        
    return results

# ==============================================================================
# WORKER FUNCTION - KNN
# ==============================================================================
def process_single_column(col_name, values, neighbors_indices, k):
    # Imports locais são vitais para o joblib no Windows
    import numpy as np
    import pandas as pd
    import warnings
    from libpysal.weights import W
    from esda.moran import Moran_Local

    # Fast fail para dados constantes
    # Check for NaNs first
    if np.isnan(values).any():
        values = np.nan_to_num(values, nan=0.0, posinf=0.0, neginf=0.0)
        
    if values.std() == 0: return pd.DataFrame()

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        
        try:
            # Reconstrói o W de forma leve (apenas com os índices recebidos)
            # Ensure indices are integers
            neighbors_dict = {i: neighbors_indices[i].tolist() for i in range(len(values))}
            w = W(neighbors_dict, silence_warnings=True)
            w.transform = 'r'
            
            # Cálculos vetorizados com NumPy (muito mais rápido que loops)
            neighbor_values = values[neighbors_indices]
            lag_mean = np.mean(neighbor_values, axis=1)
            lag_std  = np.std(neighbor_values, axis=1) # Heterogeneidade (Base para Risco)
            
            # [NEW] Diversity / Inequality Index (Coefficient of Variation)
            # Indica se a vizinhança é homogênea (0) ou diversa/desigual (alto)
            # "Indice de Diversidade Local"
            with np.errstate(divide='ignore', invalid='ignore'):
                local_cv = lag_std / (lag_mean + 1e-6)
                local_cv = np.nan_to_num(local_cv)
            
            # [NEW] Risk / Isolation Index (Absolute Difference)
            # O quão "estranho" ou "arriscado" é este ponto comparado aos vizinhos?
            # "Indice de Risco/Anomalia"
            isolation = np.abs(values - lag_mean)
            
            # Rank Local via Z-Score (Importante para normalizar Renda)
            with np.errstate(divide='ignore', invalid='ignore'):
                rank = (values - lag_mean) / (lag_std + 1e-6)
                rank = np.nan_to_num(rank)

            # LISA / Moran
            # permutations=99 provides a p-value resolution of 0.01
            # n_jobs=1 ensures we don't spawn nested threads inside the worker
            lm = Moran_Local(values, w, permutations=99, n_jobs=1)
            
            results = {
                f"{col_name}_lag_k{k}": lag_mean,
                f"{col_name}_hetero_k{k}": lag_std,
                f"{col_name}_inequality_k{k}": local_cv, # Diversity Proxy
                f"{col_name}_isolation_k{k}": isolation, # Risk/Anomaly Proxy
                f"{col_name}_rank_k{k}": rank,
                f"{col_name}_lisa_q_k{k}": lm.q,
                f"{col_name}_lisa_sig_k{k}": (lm.p_sim < 0.05).astype(int)
            }
            return pd.DataFrame(results)
            
        except Exception as e:
            # print(f"Worker Error in {col_name} K={k}: {e}")
            # Return empty DF on error to avoid crashing the whole batch
            return pd.DataFrame()

# ==============================================================================
# MAIN EXECUTION
# ==============================================================================
def main():
    start_time = time.time()
    print(f"[{time.strftime('%H:%M:%S')}] Starting Spatial Feature Engineering (Heavy Duty)...")
    
    # 1. Load Data
    print(f"[{time.strftime('%H:%M:%S')}] Loading Gold Data from {GOLD_PATH}...")
    if not os.path.exists(GOLD_PATH):
        print(f"Error: Gold file not found at {GOLD_PATH}")
        return
    df_gold = pd.read_parquet(GOLD_PATH)
    print(f"Gold loaded: {df_gold.shape}")

    print(f"[{time.strftime('%H:%M:%S')}] Loading Spatial Data from {SPATIAL_PATH}...")
    if not os.path.exists(SPATIAL_PATH):
        print(f"Error: Spatial file not found at {SPATIAL_PATH}")
        return
    gdf_spatial = gpd.read_parquet(SPATIAL_PATH)
    print(f"Spatial loaded: {gdf_spatial.shape}")

    # 2. Merge Data
    print(f"[{time.strftime('%H:%M:%S')}] Merging Spatial and Tabular Data...")
    # Ensure join keys match types
    # Assuming 'code_tract' in spatial and 'CD_SETOR' in gold
    if 'code_tract' in gdf_spatial.columns:
        # FIX: Handle float keys (remove .0)
        if pd.api.types.is_float_dtype(gdf_spatial['code_tract']):
            gdf_spatial['code_tract'] = gdf_spatial['code_tract'].astype(np.int64).astype(str)
        else:
            gdf_spatial['code_tract'] = gdf_spatial['code_tract'].astype(str)
            
    if 'CD_SETOR' in df_gold.columns:
        df_gold['CD_SETOR'] = df_gold['CD_SETOR'].astype(str)
        
    # Merge
    # Keep only necessary spatial columns to save memory
    spatial_cols = ['code_tract', 'geometry']
    if 'name_muni' in gdf_spatial.columns: spatial_cols.append('name_muni')
    
    gdf = gdf_spatial[spatial_cols].merge(df_gold, left_on='code_tract', right_on='CD_SETOR', how='inner')
    print(f"Merged Dataset: {gdf.shape}")
    
    # Clean up
    del df_gold
    del gdf_spatial
    gc.collect()

    # 3. CRS Projection
    print(f"[{time.strftime('%H:%M:%S')}] Verifying CRS...")
    if gdf.crs != TARGET_CRS:
        print(f"Projecting data from {gdf.crs} to {TARGET_CRS} (Meters)...")
        gdf = gdf.to_crs(TARGET_CRS)
    else:
        print(f"CRS is already {TARGET_CRS}.")

    # 4. Prepare Data for Processing
    # Select numeric columns only
    numeric_cols = gdf.select_dtypes(include=[np.number]).columns.tolist()

    # ---------------------------------------------------------
    # FILTER: Select only High-Value "Expert" features + Income
    # Voltando ao plano Diamond: ~258 features de alta inteligência.
    
    # 1. Expert Features (The "Brain" of the dataset)
    expert_cols = [c for c in numeric_cols if 'expert_' in c]
    
    # 2. Calculated Features (Standard metrics)
    calc_cols = [c for c in numeric_cols if 'calc_' in c]
    
    # 3. Key Mass Columns (For clustering wealth/density)
    mass_cols = [
        'massa_salarial_total', 
        'total_de_pessoas_v0001',
        'total_de_domicilios_particulares_dppo_dppv_dppuo_dpio_v0003'
    ]
    mass_cols = [c for c in mass_cols if c in numeric_cols]
    
    # Combine all targets
    target_cols = list(set(expert_cols + calc_cols + mass_cols))
    
    # Fallback: If no expert/calc columns found (e.g. raw data), try to find them by content
    if len(target_cols) < 5:
        print("Warning: No 'expert_' or 'calc_' columns found. Attempting to identify by keywords...")
        # Add income if not present
        if 'rendimento_medio_responsavel' in numeric_cols: target_cols.append('rendimento_medio_responsavel')
        if 'rendimento_medio_responsavel_sm' in numeric_cols: target_cols.append('rendimento_medio_responsavel_sm')
        
    print(f"Selected {len(target_cols)} Priority Columns for processing (Diamond Plan).")
    print(f"   - Expert: {len(expert_cols)}")
    print(f"   - Calculated: {len(calc_cols)}")
    print(f"   - Mass: {len(mass_cols)}")

    # Extract coordinates for W generation
    print(f"[{time.strftime('%H:%M:%S')}] Extracting coordinates...")
    # FIX: Use centroids for polygons
    centroids = gdf.geometry.centroid
    coords = np.column_stack((centroids.x, centroids.y))
    
    # Build Tree once
    print(f"[{time.strftime('%H:%M:%S')}] Building cKDTree...")
    tree = cKDTree(coords)

    # ---------------------------------------------------------
    # 4.5 GRAVITY FEATURES (Distance Decay)
    # ---------------------------------------------------------
    print(f"\n[{time.strftime('%H:%M:%S')}] Calculating Gravity Features (Hansen Accessibility)...")
    
    # Definição dos Raios (em Metros) e Beta de Decaimento
    GRAVITY_RADII = [1000, 2000, 5000] # 1km, 2km, 5km
    GRAVITY_BETA = 1.5
    
    # Identificar colunas de Massa (Riqueza e Pessoas)
    # Tenta achar os nomes comuns do Censo. Ajuste se necessário.
    potential_gravity_cols = [
        'massa_salarial_total', 
        'total_de_pessoas_v0001',
        'total_de_domicilios_particulares_dppo_dppv_dppuo_dpio_v0003'
    ]
    gravity_cols = [c for c in potential_gravity_cols if c in gdf.columns]
    
    if not gravity_cols:
        print("Warning: No mass columns found for Gravity (massa_salarial, populacao). Skipping.")
    else:
        print(f"Gravity Targets: {gravity_cols}")
        
        for r in GRAVITY_RADII:
            print(f"   > Processing Radius {r}m...")
            
            # Query eficiente para raio fixo
            # query_ball_point retorna lista de listas de indices
            indices_list = tree.query_ball_point(coords, r=r)
            
            # Como query_ball_point não retorna distâncias, e calcular 300k distancias é lento,
            # vamos fazer uma aproximação vetorizada inteligente ou loop otimizado.
            # Para manter "Heavy" mas rápido, vamos iterar (infelizmente necessário para geometria variável)
            # mas usando list comprehension que é rápida.
            
            for col in gravity_cols:
                print(f"      Calculating for {col}...")
                # Prepara o array de valores para acesso rápido
                mass_values = gdf[col].values
                
                # Parallelize Gravity Calculation
                # Split indices_list into chunks
                # We need to pass (index, neighbors) to keep track of order, or just split and reassemble
                
                # Create chunks of (original_index, neighbors)
                chunk_size = len(indices_list) // N_JOBS + 1
                chunks = []
                for i in range(0, len(indices_list), chunk_size):
                    # Create a list of tuples (original_idx, neighbors) for this chunk
                    chunk_data = list(enumerate(indices_list[i:i+chunk_size], start=i))
                    chunks.append(chunk_data)
                
                try:
                    chunk_results = Parallel(n_jobs=N_JOBS, backend=BACKEND, verbose=0)(
                        delayed(calculate_gravity_chunk)(chunk, coords, mass_values, GRAVITY_BETA)
                        for chunk in chunks
                    )
                    
                    # Flatten results
                    decay_sum = np.concatenate(chunk_results)
                    
                    # Salva a feature
                    gdf[f'gravity_{col}_{r}m'] = decay_sum
                    
                except Exception as e:
                    print(f"Error in Gravity Parallel: {e}")
                    # Fallback to sequential if parallel fails
                    decay_sum = np.zeros(len(gdf))
                    for i, neighbors in enumerate(indices_list):
                        if not neighbors: continue
                        center_pt = coords[i]
                        neigh_pts = coords[neighbors]
                        dists = np.linalg.norm(neigh_pts - center_pt, axis=1)
                        dists = np.maximum(dists, 1.0)
                        masses = mass_values[neighbors]
                        decay_sum[i] = np.sum(masses / (dists ** GRAVITY_BETA))
                    gdf[f'gravity_{col}_{r}m'] = decay_sum

    print(f"[{time.strftime('%H:%M:%S')}] Gravity calculation done.")

    # 5. Parallel KNN & LISA Processing
    print(f"[{time.strftime('%H:%M:%S')}] Starting Parallel Processing (n_jobs={N_JOBS})...")
    
    all_new_features = []

    for k in KNN_KS:
        k_start = time.time()
        print(f"\n--- Processing K={k} ---")
        
        # Query Tree for Indices
        print(f"Querying cKDTree for k={k}...")
        # k+1 because the first neighbor is the point itself
        dists, indices = tree.query(coords, k=k+1) 
        
        # [FEATURE EXTRA] Urban Density Proxy
        # Salva a média da distância até os vizinhos. 
        # Baixo valor = Alta Densidade Urbana. Alto valor = Rural/Esparso.
        # axis=1 faz a média por linha (setor). Ignora a coluna 0 (self).
        mean_dist = np.mean(dists[:, 1:], axis=1)
        all_new_features.append(pd.DataFrame({f'geo_avg_dist_k{k}': mean_dist}))

        # Remove the point itself (first column)
        neighbor_indices = indices[:, 1:]
        
        # Prepare data for workers
        # We pass the indices matrix and the numpy arrays
        print(f"Dispatching {len(target_cols)} tasks to workers...")
        
        results = None
        try:
            results = Parallel(n_jobs=N_JOBS, backend=BACKEND, verbose=5)(
                delayed(process_single_column)(col, gdf[col].values, neighbor_indices, k)
                for col in target_cols
            )
            
            # Concatenate results for this K
            print("Concatenating results...")
            # Filter out empty results (from fast fail)
            results = [r for r in results if not r.empty]
            
            if results:
                k_df = pd.concat(results, axis=1)
                all_new_features.append(k_df)
            
        except Exception as e:
            print(f"Error during parallel execution for K={k}: {e}")
            import traceback
            traceback.print_exc()
        
        # Cleanup to free memory
        del neighbor_indices
        del dists
        del indices
        if results:
            del results
        gc.collect()
        
        print(f"Finished K={k} in {time.time() - k_start:.2f}s")

    # 5. Merge and Save
    print(f"\n[{time.strftime('%H:%M:%S')}] Merging all features...")
    if all_new_features:
        final_features = pd.concat(all_new_features, axis=1)
        
        # Combine with original geometry and IDs (or full gdf)
        # Using full gdf might be memory intensive if it's huge. 
        # If you only need the new features + ID, adjust here.
        final_gdf = pd.concat([gdf, final_features], axis=1)
        
        print(f"Saving to {OUTPUT_FILE}...")
        # Ensure directory exists
        os.makedirs(os.path.dirname(os.path.abspath(OUTPUT_FILE)), exist_ok=True)
        
        final_gdf.to_parquet(OUTPUT_FILE)
        print(f"Success! Saved {final_gdf.shape[1]} columns.")
    else:
        print("No features were generated.")

    print(f"Total execution time: {time.time() - start_time:.2f}s")

if __name__ == "__main__":
    # Windows requires this guard for multiprocessing
    main()
