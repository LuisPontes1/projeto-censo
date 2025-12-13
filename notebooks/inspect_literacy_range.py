import pandas as pd
import os

dict_path = r'C:\projetos\projeto-censo\data\raw\dicionario_de_dados_agregados_por_setores_censitarios_20250417.xlsx'

try:
    xls = pd.ExcelFile(dict_path)
    target_sheet = 'Dicionário não PCT'
    if target_sheet in xls.sheet_names:
        df = pd.read_excel(dict_path, sheet_name=target_sheet)
        
        # Find index of V00748
        mask = df.iloc[:, 2].astype(str).str.contains('V00748', na=False)
        if mask.any():
            idx = mask.idxmax()
            # Print 20 rows before and 20 after
            print(df.iloc[idx-20:idx+20, :5].to_string())

except Exception as e:
    print(f"Error: {e}")
