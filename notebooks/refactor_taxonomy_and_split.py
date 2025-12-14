import pandas as pd
import geopandas as gpd
import os

# Paths
DIAMOND_FILE = r"c:\projetos\projeto-censo\data\diamond\censo_2022_diamond_features.parquet"
GOLD_FILE = r"c:\projetos\projeto-censo\data\gold\censo_2022_features_final.parquet"
RAW_SPATIAL_FILE = r"c:\projetos\projeto-censo\data\spatial\malha_setores_2022.parquet"

# Outputs
OUTPUT_MESH = r"c:\projetos\projeto-censo\data\spatial\malha_setores_2022_final.parquet"
OUTPUT_DIAMOND_FEATURES = r"c:\projetos\projeto-censo\data\diamond\censo_2022_diamond_features_final.parquet"
OUTPUT_GOLD_FEATURES = r"c:\projetos\projeto-censo\data\gold\censo_2022_gold_standardized.parquet"

def standardize_taxonomy(df, is_spatial=False):
    """
    Applies the project taxonomy:
    - id_*: Codes (String)
    - nm_*: Names
    - Lowercase columns
    """
    # Map for Raw Spatial columns
    spatial_map = {
        'code_tract': 'id_setor',
        'code_muni': 'id_municipio',
        'name_muni': 'nm_municipio',
        'code_district': 'id_distrito',
        'name_district': 'nm_distrito',
        'code_subdistrict': 'id_subdistrito',
        'name_subdistrict': 'nm_subdistrito',
        'code_neighborhood': 'id_bairro',
        'name_neighborhood': 'nm_bairro',
        'code_state': 'id_uf',
        'name_state': 'nm_uf',
        'code_region': 'id_regiao',
        'name_region': 'nm_regiao',
        'area_km2': 'area_km2'
    }
    
    # Map for Gold/Diamond columns (which might use CD_SETOR)
    feature_map = {
        'CD_SETOR': 'id_setor',
        'NM_MUN': 'nm_municipio',
        'NM_UF': 'nm_uf',
        'code_tract': 'id_setor', # In case it exists
        'name_muni': 'nm_municipio'
    }
    
    # Apply renaming
    if is_spatial:
        df = df.rename(columns=spatial_map)
    else:
        df = df.rename(columns=feature_map)
        
    # Lowercase all columns
    df.columns = [c.lower() for c in df.columns]
    
    # Remove duplicate columns (keep first)
    df = df.loc[:, ~df.columns.duplicated()]
    
    # Ensure id_setor is string and clean
    if 'id_setor' in df.columns:
        df['id_setor'] = df['id_setor'].astype(str)
        # Remove .0 if present (from float conversion)
        df['id_setor'] = df['id_setor'].str.replace(r'\.0$', '', regex=True)
        
    return df

def main():
    # ---------------------------------------------------------
    # 1. Create the Master Mesh (Cadastro)
    # ---------------------------------------------------------
    print("Loading Raw Spatial Data (The Registry)...")
    gdf_spatial = gpd.read_parquet(RAW_SPATIAL_FILE)
    print(f"Raw Spatial Shape: {gdf_spatial.shape}")
    
    # Apply Taxonomy
    gdf_mesh = standardize_taxonomy(gdf_spatial, is_spatial=True)
    
    # Select Registry Columns
    registry_cols = [
        'id_setor', 'geometry', 
        'id_municipio', 'nm_municipio',
        'id_distrito', 'nm_distrito',
        'id_subdistrito', 'nm_subdistrito',
        'id_bairro', 'nm_bairro',
        'id_uf', 'nm_uf',
        'id_regiao', 'nm_regiao',
        'area_km2'
    ]
    # Filter only existing columns
    registry_cols = [c for c in registry_cols if c in gdf_mesh.columns]
    
    gdf_mesh = gdf_mesh[registry_cols].copy()
    
    print(f"Saving Master Mesh (Cadastro) to {OUTPUT_MESH}...")
    os.makedirs(os.path.dirname(OUTPUT_MESH), exist_ok=True)
    gdf_mesh.to_parquet(OUTPUT_MESH)
    print(f"Mesh saved: {gdf_mesh.shape}")

    # ---------------------------------------------------------
    # 2. Process GOLD Dataset
    # ---------------------------------------------------------
    print("\nLoading Gold Dataset...")
    try:
        df_gold = pd.read_parquet(GOLD_FILE)
        df_gold = standardize_taxonomy(df_gold, is_spatial=False)
        
        # Remove geometry/registry info if present to keep it pure features
        cols_to_drop = [c for c in df_gold.columns if c in registry_cols and c != 'id_setor']
        if cols_to_drop:
            df_gold = df_gold.drop(columns=cols_to_drop)
            
        print(f"Saving Gold to {OUTPUT_GOLD_FEATURES}...")
        df_gold.to_parquet(OUTPUT_GOLD_FEATURES)
    except Exception as e:
        print(f"Error processing Gold: {e}")

    # ---------------------------------------------------------
    # 3. Process DIAMOND Dataset
    # ---------------------------------------------------------
    print("\nLoading Diamond Dataset...")
    # Load as pandas (ignore geometry, we have it in Mesh)
    df_diamond = pd.read_parquet(DIAMOND_FILE)
    
    # If it has geometry, drop it immediately to save memory/confusion
    if 'geometry' in df_diamond.columns:
        df_diamond = df_diamond.drop(columns=['geometry'])
        
    df_diamond = standardize_taxonomy(df_diamond, is_spatial=False)
    
    # Remove registry info (names of munis, ufs etc) to avoid duplication with Mesh
    # Keep only id_setor and the metrics
    cols_to_drop = [c for c in df_diamond.columns if c in registry_cols and c != 'id_setor']
    if cols_to_drop:
        print(f"Dropping redundant registry columns from Diamond: {cols_to_drop}")
        df_diamond = df_diamond.drop(columns=cols_to_drop)
    
    # Save Features
    print(f"Saving Diamond Features to {OUTPUT_DIAMOND_FEATURES}...")
    df_diamond.to_parquet(OUTPUT_DIAMOND_FEATURES)
    
    print("\nSuccess! Taxonomy applied. Mesh contains Registry. Features contain Metrics.")

if __name__ == "__main__":
    main()
