import pandas as pd
import os

csv_path = r'C:\projetos\projeto-censo\data\raw\Agregados_por_setores_alfabetizacao_BR.csv'

# Read first 100 rows
df = pd.read_csv(csv_path, sep=';', nrows=100, dtype=str)

# Convert to numeric
cols_to_check = ['V00644', 'V00748'] # 15-19 Total vs Literate
for c in cols_to_check:
    df[c] = pd.to_numeric(df[c].str.replace(',', '.'), errors='coerce')

print(df[cols_to_check].head(10))
print("\nCheck V00644 >= V00748:")
print((df['V00644'] >= df['V00748']).value_counts())
