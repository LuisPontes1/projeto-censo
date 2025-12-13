import pandas as pd
import os

dict_path = r'C:\projetos\projeto-censo\data\raw\dicionario_de_dados_agregados_por_setores_censitarios_20250417.xlsx'

try:
    xls = pd.ExcelFile(dict_path)
    target_sheet = 'Dicionário não PCT'
    if target_sheet in xls.sheet_names:
        df = pd.read_excel(dict_path, sheet_name=target_sheet)
        
        print("--- LITERACY DETAILS ---")
        # Get rows 643 to 660
        print(df.iloc[643:660, :5].to_string())
        
        print("\n--- DOMICILIO 3 (Assets) ---")
        # Search for Domicilio 3 related keywords or just scan for assets again with more context
        keywords = ['Geladeira', 'Máquina de lavar', 'Automóvel', 'Motocicleta', 'Internet']
        for kw in keywords:
            mask = df.apply(lambda x: x.astype(str).str.contains(kw, case=False, na=False))
            if mask.any().any():
                row_idx = mask.any(axis=1).idxmax()
                print(f"\nFound {kw} at {row_idx}:")
                print(df.iloc[row_idx:row_idx+3, :5].to_string())

except Exception as e:
    print(f"Error: {e}")
