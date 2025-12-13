import pandas as pd
import os

dict_path = r'C:\projetos\projeto-censo\data\raw\dicionario_de_dados_agregados_por_setores_censitarios_20250417.xlsx'

try:
    xls = pd.ExcelFile(dict_path)
    target_sheet = 'Dicionário não PCT'
    if target_sheet in xls.sheet_names:
        df = pd.read_excel(dict_path, sheet_name=target_sheet)
        
        # Look for the start of Literacy section to find the Total Literate variable
        # Usually before the age breakdown
        print("--- LITERACY HEADER ---")
        mask_alf = df.apply(lambda x: x.astype(str).str.contains('Alfabetização', case=False, na=False))
        if mask_alf.any().any():
            first_idx = mask_alf.any(axis=1).idxmax()
            # Print 20 rows starting 10 before the first match
            start = max(0, first_idx - 10)
            print(df.iloc[start:first_idx+10, :5].to_string())

except Exception as e:
    print(f"Error: {e}")
