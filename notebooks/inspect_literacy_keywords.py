import pandas as pd
import os

dict_path = r'C:\projetos\projeto-censo\data\raw\dicionario_de_dados_agregados_por_setores_censitarios_20250417.xlsx'

try:
    xls = pd.ExcelFile(dict_path)
    target_sheet = 'Dicionário não PCT'
    if target_sheet in xls.sheet_names:
        df = pd.read_excel(dict_path, sheet_name=target_sheet)
        
        print("--- SEARCHING FOR 'ALFABETIZADAS' ---")
        mask = df.apply(lambda x: x.astype(str).str.contains('alfabetizadas', case=False, na=False))
        if mask.any().any():
            # Get all rows that match
            rows = df[mask.any(axis=1)]
            print(f"Found {len(rows)} rows.")
            print(rows.iloc[:10, :5].to_string())
            
            # Check if there is a 'Total'
            total_row = rows[rows.iloc[:, 3].astype(str).str.contains('Total', case=False, na=False)]
            if not total_row.empty:
                print("\nPossible Total Variables:")
                print(total_row.iloc[:, :5].to_string())

except Exception as e:
    print(f"Error: {e}")
