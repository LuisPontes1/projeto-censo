import pandas as pd
import os

DATA_DIR = r"c:\projetos\projeto-censo\data\raw"
DICT_FILE = os.path.join(DATA_DIR, "dicionario_de_dados_agregados_por_setores_censitarios_20250417.xlsx")

xls = pd.ExcelFile(DICT_FILE)
all_vars = []

for sheet in xls.sheet_names:
    if 'Siglas' in sheet: continue
    df = pd.read_excel(DICT_FILE, sheet_name=sheet)
    if 'Variável' in df.columns:
        all_vars.extend(df['Variável'].astype(str).str.upper().tolist())

print(f"Total variables: {len(all_vars)}")
print(f"Unique variables: {len(set(all_vars))}")

if len(all_vars) != len(set(all_vars)):
    print("Warning: Duplicate variables found!")
    from collections import Counter
    c = Counter(all_vars)
    print([k for k, v in c.items() if v > 1])
