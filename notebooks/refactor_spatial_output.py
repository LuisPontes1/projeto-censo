import pandas as pd
import os

# Paths
DIAMOND_FILE = r"c:\projetos\projeto-censo\data\diamond\censo_2022_diamond_features.parquet"
OUTPUT_SPATIAL_FILE = r"c:\projetos\projeto-censo\data\diamond\censo_2022_spatial_features_only.parquet"
GOLD_FILE = r"c:\projetos\projeto-censo\data\gold\censo_2022_features_final.parquet"

def main():
    print("Loading Diamond Dataset...")
    if not os.path.exists(DIAMOND_FILE):
        print(f"Error: {DIAMOND_FILE} not found.")
        return

    df_diamond = pd.read_parquet(DIAMOND_FILE)
    print(f"Diamond Shape: {df_diamond.shape}")
    
    # Load Gold columns to identify what is "original" and what is "new"
    print("Loading Gold columns for comparison...")
    df_gold = pd.read_parquet(GOLD_FILE)
    gold_cols = set(df_gold.columns)
    
    # Identify new columns
    all_cols = set(df_diamond.columns)
    new_cols = list(all_cols - gold_cols)
    
    # Ensure CD_SETOR is in the list for joining
    if 'CD_SETOR' not in new_cols:
        new_cols.insert(0, 'CD_SETOR')
        
    # Also keep geometry if present in Diamond but not Gold (it usually is in Spatial)
    # But user asked for "colunas e geográficos separados".
    # So we save the FEATURES in one file. The Geometry is already in 'malha_setores_2022.parquet'.
    
    # Filter
    print(f"Extracting {len(new_cols)} spatial features...")
    df_spatial_features = df_diamond[new_cols].copy()
    
    # Save
    print(f"Saving to {OUTPUT_SPATIAL_FILE}...")
    df_spatial_features.to_parquet(OUTPUT_SPATIAL_FILE)
    print("Success! Separated spatial features from the main dataset.")

if __name__ == "__main__":
    main()
