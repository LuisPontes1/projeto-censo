import pandas as pd
import os

dict_path = r'C:\projetos\projeto-censo\data\raw\dicionario_de_dados_agregados_por_setores_censitarios_20250417.xlsx'

try:
    xls = pd.ExcelFile(dict_path)
    print(xls.sheet_names)
    
    # Check 'Dicionário Básico' or others
    for sheet in xls.sheet_names:
        df = pd.read_excel(dict_path, sheet_name=sheet)
        mask = df.apply(lambda x: x.astype(str).str.contains('V05000', na=False))
        if mask.any().any():
            print(f"Found V05000 in sheet: {sheet}")
            idx = mask.any(axis=1).idxmax()
            print(df.iloc[idx:idx+10, :5].to_string())

except Exception as e:
    print(f"Error: {e}")
