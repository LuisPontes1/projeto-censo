import pandas as pd

dict_path = r'C:\projetos\projeto-censo\data\raw\dicionario_de_dados_agregados_por_setores_censitarios_20250417.xlsx'

print(f"Lendo dicionário: {dict_path}")

try:
    # Read all sheets to find the one for Domicilio 3
    xls = pd.ExcelFile(dict_path)
    print("Planilhas disponíveis:", xls.sheet_names)
    
    # Assuming there's a sheet named 'Domicilio03' or similar
    target_sheet = None
    for sheet in xls.sheet_names:
        if 'domicilio' in sheet.lower() and '3' in sheet:
            target_sheet = sheet
            break
    
    if target_sheet:
        print(f"Analisando planilha: {target_sheet}")
        df = pd.read_excel(xls, sheet_name=target_sheet)
        
        # Print first few rows to understand structure
        print(df.head())
        
        # Filter for relevant columns (Code and Description)
        # Usually columns are like 'Nome da Variável', 'Descrição'
        print("\n--- Variáveis de Interesse (Bens Duráveis / Serviços) ---")
        for index, row in df.iterrows():
            desc = str(row.iloc[1]).lower() # Assuming description is in 2nd column
            if any(x in desc for x in ['internet', 'automóvel', 'carro', 'geladeira', 'máquina de lavar', 'televisão', 'moto']):
                print(f"{row.iloc[0]}: {row.iloc[1]}")
                
    else:
        print("Planilha Domicilio 3 não encontrada.")

except Exception as e:
    print(f"Erro: {e}")
