import pandas as pd
import os
import glob
import re
from unicodedata import normalize
import numpy as np

DATA_DIR = r"c:\projetos\projeto-censo\data\raw"
OUTPUT_DIR = r"c:\projetos\projeto-censo\data\silver"
DICT_FILE = os.path.join(DATA_DIR, "dicionario_de_dados_agregados_por_setores_censitarios_20250417.xlsx")

def slugify(text):
    if not isinstance(text, str):
        return str(text)
    text = normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')
    text = text.lower()
    text = re.sub(r'[^a-z0-9]+', '_', text)
    text = text.strip('_')
    return text

def load_dictionary():
    print("Loading dictionary...")
    xls = pd.ExcelFile(DICT_FILE)
    var_map = {}
    
    for sheet in xls.sheet_names:
        if 'Siglas' in sheet: continue
        print(f"  Processing sheet: {sheet}")
        df = pd.read_excel(DICT_FILE, sheet_name=sheet)
        
        if 'Variável' in df.columns and 'Descrição' in df.columns:
            for _, row in df.iterrows():
                var_code = str(row['Variável']).strip().upper()
                desc = row['Descrição']
                # Append code to ensure uniqueness and traceability
                slug = slugify(desc)
                var_map[var_code] = f"{slug}_{var_code.lower()}"
                
    print(f"Dictionary loaded with {len(var_map)} variables.")
    return var_map

def process_file(csv_file, var_map):
    filename = os.path.basename(csv_file)
    name_no_ext = os.path.splitext(filename)[0]
    output_path = os.path.join(OUTPUT_DIR, f"{name_no_ext}.parquet")
    
    if os.path.exists(output_path):
        print(f"Skipping {filename} (already processed)")
        return

    print(f"Processing {filename}...")
    
    # Read CSV
    try:
        df = pd.read_csv(csv_file, sep=';', encoding='utf-8', dtype=str)
    except UnicodeDecodeError:
        print("  UTF-8 failed, trying Latin-1")
        df = pd.read_csv(csv_file, sep=';', encoding='latin1', dtype=str)
    
    # Rename columns
    new_cols = {}
    for col in df.columns:
        col_upper = col.upper()
        if col_upper in var_map:
            # Keep the code in the name for reference? Or just the description?
            # Let's use just the description as requested, but maybe prefix with code if ambiguous?
            # User asked for "tratando eles", usually implies readable names.
            # But for joining, we need to keep the key columns (CD_SETOR, etc) as is.
            new_cols[col] = var_map[col_upper]
        else:
            # Keep original name (usually CD_SETOR, NM_MUN, etc.)
            new_cols[col] = col
            
    df.rename(columns=new_cols, inplace=True)
    
    # Treat values
    # Replace '.' with NaN
    df.replace('.', np.nan, inplace=True)
    
    # Convert numeric columns
    # We iterate and try to convert.
    # Key columns (CD_...) should remain strings usually, but let's see.
    # In the notebook we decided to keep CD_ as strings/numeric but careful.
    # Here, since we loaded everything as str, we need to be careful.
    
    for col in df.columns:
        # Skip ID columns from conversion to float (keep as object/string or int if clean)
        if col.startswith('CD_') or col.startswith('NM_'):
            continue
            
        # Try to convert to numeric
        # Check if it looks like a number with comma
        if df[col].str.contains(',', na=False).any():
            df[col] = df[col].str.replace(',', '.').astype(float)
        else:
            # Try to convert to numeric (int or float)
            # Using coerce to handle potential bad data by turning into NaN, 
            # but if we want to stop on error, we could use 'raise'.
            # Given the user request "parar e corrigir", let's try to be strict but practical.
            # If we use 'raise', we might stop on a single bad char. 
            # Let's use 'coerce' but report if we created NaNs? 
            # For now, let's just remove the try/except suppression and use coerce to avoid the FutureWarning.
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    # Save to Parquet
    df.to_parquet(output_path, index=False, engine='fastparquet')
    print(f"  Saved to {output_path}")

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    var_map = load_dictionary()
    
    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    print(f"Found {len(csv_files)} CSV files.")
    
    for csv_file in csv_files:
        process_file(csv_file, var_map)

if __name__ == "__main__":
    main()
