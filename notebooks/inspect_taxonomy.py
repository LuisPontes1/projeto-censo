import pandas as pd

DIAMOND_FILE = r"c:\projetos\projeto-censo\data\diamond\censo_2022_diamond_features.parquet"

def inspect(file_path, name):
    print(f"\n--- Inspecting {name} ---")
    try:
        df = pd.read_parquet(file_path)
        print("Columns with 'district':")
        print([c for c in df.columns if 'district' in c.lower()])
    except Exception as e:
        print(f"Error reading {name}: {e}")

inspect(DIAMOND_FILE, "Diamond")
