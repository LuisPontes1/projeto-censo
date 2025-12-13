import pandas as pd
import os

dict_path = r'C:\projetos\projeto-censo\data\raw\dicionario_de_dados_agregados_por_setores_censitarios_20250417.xlsx'

try:
    # Read all sheets to find the one with Parentesco
    xls = pd.ExcelFile(dict_path)
    print("Sheets:", xls.sheet_names)
    
    for sheet in xls.sheet_names:
        if 'parentesco' in sheet.lower() or 'agregados' in sheet.lower():
            df = pd.read_excel(dict_path, sheet_name=sheet)
            # Look for V01042
            row = df[df.iloc[:, 0].astype(str).str.contains('V01042', na=False)]
            if not row.empty:
                print(f"--- Sheet: {sheet} ---")
                # Print V01042 and next few rows
                start_idx = row.index[0]
                print(df.iloc[start_idx:start_idx+10, :2].to_string())
                break
except Exception as e:
    print(f"Error: {e}")
