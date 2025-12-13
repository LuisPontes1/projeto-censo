import pandas as pd

dict_path = r'C:\projetos\projeto-censo\data\raw\dicionario_de_dados_agregados_por_setores_censitarios_20250417.xlsx'

print(f"Lendo dicionário: {dict_path}")

try:
    xls = pd.ExcelFile(dict_path)
    target_sheet = 'Dicionário não PCT'
    
    print(f"Analisando planilha: {target_sheet}")
    df = pd.read_excel(xls, sheet_name=target_sheet)
    
    # Search for Domicilio 3 related terms in the dataframe
    print("\n--- Procurando por variáveis de bens (excluindo carro-pipa) ---")
    
    keywords = ['internet', 'automóvel', 'geladeira', 'máquina de lavar', 'televisão', 'motocicleta']
    
    found_count = 0
    for index, row in df.iterrows():
        row_str = str(row.values).lower()
        if any(k in row_str for k in keywords) and 'carro-pipa' not in row_str:
            print(f"Row {index}: {row.values}")
            found_count += 1
            if found_count > 50:
                break
                
    if found_count == 0:
        print("Nenhuma variável de bens encontrada. Verificando se existem variáveis de 'Bens' ou 'Equipamentos'.")
        # Check for generic terms
        for index, row in df.iterrows():
            if 'bens' in str(row.values).lower() or 'equipamento' in str(row.values).lower():
                 print(f"Generic Row {index}: {row.values}")
                 found_count += 1
                 if found_count > 10: break

except Exception as e:
    print(f"Erro: {e}")
