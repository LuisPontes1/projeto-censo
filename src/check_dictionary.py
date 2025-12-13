import pandas as pd
import os

DATA_DIR = r"c:\projetos\projeto-censo\data\raw"
DICT_FILE = os.path.join(DATA_DIR, "dicionario_de_dados_agregados_por_setores_censitarios_20250417.xlsx")

xls = pd.ExcelFile(DICT_FILE)
print("Sheets:", xls.sheet_names)

for sheet in xls.sheet_names:
    print(f"\n--- Sheet: {sheet} ---")
    df = pd.read_excel(DICT_FILE, sheet_name=sheet)
    print(df.columns.tolist())
    print(df.head())
