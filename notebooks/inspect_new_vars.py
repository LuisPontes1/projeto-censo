import pandas as pd
import os

dict_path = r'C:\projetos\projeto-censo\data\raw\dicionario_de_dados_agregados_por_setores_censitarios_20250417.xlsx'

try:
    xls = pd.ExcelFile(dict_path)
    target_sheet = 'Dicionário não PCT'
    if target_sheet in xls.sheet_names:
        df = pd.read_excel(dict_path, sheet_name=target_sheet)
        
        print("--- SEARCHING FOR LITERACY (Alfabetização) ---")
        mask_alf = df.apply(lambda x: x.astype(str).str.contains('Alfabetização', case=False, na=False))
        if mask_alf.any().any():
            row_idx = mask_alf.any(axis=1).idxmax()
            print(df.iloc[row_idx:row_idx+10, :2].to_string())
            
        print("\n--- SEARCHING FOR ASSETS (Carro, Geladeira, Internet) ---")
        keywords = ['Geladeira', 'Máquina de lavar', 'Automóvel', 'Motocicleta', 'Internet']
        for kw in keywords:
            mask = df.apply(lambda x: x.astype(str).str.contains(kw, case=False, na=False))
            if mask.any().any():
                row_idx = mask.any(axis=1).idxmax()
                print(f"\nFound {kw}:")
                print(df.iloc[row_idx:row_idx+5, :2].to_string())

except Exception as e:
    print(f"Error: {e}")
