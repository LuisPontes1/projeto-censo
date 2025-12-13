import pandas as pd
import os

dict_path = r'C:\projetos\projeto-censo\data\raw\dicionario_de_dados_agregados_por_setores_censitarios_20250417.xlsx'

try:
    xls = pd.ExcelFile(dict_path)
    target_sheet = 'Dicionário não PCT'
    if target_sheet in xls.sheet_names:
        df = pd.read_excel(dict_path, sheet_name=target_sheet)
        
        # Search for V01042 in any column
        mask = df.apply(lambda x: x.astype(str).str.contains('V01042', na=False))
        if mask.any().any():
            row_idx = mask.any(axis=1).idxmax()
            print(f"Found V01042 at index {row_idx}")
            # Print surrounding rows (columns 0 and 1 usually have code and desc)
            print(df.iloc[row_idx:row_idx+20, :2].to_string())
        else:
            print("V01042 not found.")

except Exception as e:
    print(f"Error: {e}")
