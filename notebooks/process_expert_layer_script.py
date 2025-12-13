import pandas as pd
import numpy as np
import os

# Configuração de Caminhos
data_dir = r'C:\projetos\projeto-censo\data\silver'
output_dir = r'C:\projetos\projeto-censo\data\gold'
os.makedirs(output_dir, exist_ok=True)

input_path = os.path.join(data_dir, 'censo_2022_agregado_massivo.parquet')
output_path = os.path.join(output_dir, 'censo_2022_features_final.parquet')

print(f"Carregando dataset massivo: {input_path}")
df_final_massive = pd.read_parquet(input_path)
print(f"Shape inicial: {df_final_massive.shape}")

# --- 1. CAMADA EXPERT (Índices Compostos) ---
print("\n--- Calculando Índices Compostos ---")

# Razão de Dependência
if 'pop_0_19' in df_final_massive.columns and 'pop_70_plus' in df_final_massive.columns and 'pop_20_69' in df_final_massive.columns:
    df_final_massive['razao_dependencia'] = (df_final_massive['pop_0_19'] + df_final_massive['pop_70_plus']) / (df_final_massive['pop_20_69'] + 1e-6)
    print("Razão de Dependência calculada.")

# Índice de Envelhecimento
if 'pop_70_plus' in df_final_massive.columns and 'pop_0_19' in df_final_massive.columns:
    df_final_massive['indice_envelhecimento'] = df_final_massive['pop_70_plus'] / (df_final_massive['pop_0_19'] + 1e-6)
    print("Índice de Envelhecimento calculado.")

# Diversidade Racial (Simpson)
cols_raca_pct = ['pct_branca', 'pct_preta', 'pct_parda']
existing_raca_cols = [c for c in cols_raca_pct if c in df_final_massive.columns]
if len(existing_raca_cols) >= 1:
    sum_sq = df_final_massive[existing_raca_cols].pow(2).sum(axis=1)
    pct_outros = 1 - df_final_massive[existing_raca_cols].sum(axis=1)
    pct_outros = pct_outros.clip(lower=0)
    sum_sq += pct_outros**2
    df_final_massive['indice_diversidade_racial'] = 1 - sum_sq
    print("Índice de Diversidade Racial calculado.")

# Potencial de Consumo
if 'renda_per_capita_sm' in df_final_massive.columns and 'densidade_demografica' in df_final_massive.columns:
    log_densidade = np.log1p(df_final_massive['densidade_demografica'])
    df_final_massive['indice_potencial_consumo'] = df_final_massive['renda_per_capita_sm'] * log_densidade
    print("Índice de Potencial de Consumo calculado.")

# --- 2. MORTALIDADE ---
print("\n--- Integrando Mortalidade ---")
path_obitos = os.path.join(data_dir, 'Agregados_por_setores_obitos_BR.parquet')
if os.path.exists(path_obitos):
    df_obitos = pd.read_parquet(path_obitos)
    if 'CD_SETOR' in df_obitos.columns:
        df_obitos['CD_SETOR'] = df_obitos['CD_SETOR'].astype(str)
        if 'V01226' in df_obitos.columns and 'V01227' in df_obitos.columns:
            df_obitos['total_obitos'] = df_obitos['V01226'] + df_obitos['V01227']
            df_final_massive = df_final_massive.merge(df_obitos[['CD_SETOR', 'total_obitos']], on='CD_SETOR', how='left')
            
            if 'total_de_pessoas_v0001' in df_final_massive.columns:
                df_final_massive['taxa_mortalidade_1000'] = (df_final_massive['total_obitos'] / df_final_massive['total_de_pessoas_v0001']) * 1000
                df_final_massive['taxa_mortalidade_1000'] = df_final_massive['taxa_mortalidade_1000'].fillna(0)
                print("Taxa de Mortalidade calculada.")
else:
    print("Arquivo de Óbitos não encontrado.")

# --- 3. ESTRUTURA FAMILIAR (Parentesco) ---
print("\n--- Integrando Parentesco ---")
path_parentesco = os.path.join(data_dir, 'Agregados_por_setores_parentesco_BR.parquet')
if os.path.exists(path_parentesco):
    df_par = pd.read_parquet(path_parentesco)
    if 'CD_SETOR' in df_par.columns:
        df_par['CD_SETOR'] = df_par['CD_SETOR'].astype(str)
        
        # V01042: Responsável
        # V01043/V01044: Cônjuges
        # V01046: Filho só do responsável
        # V01049: Pais/Sogros
        # V01051: Netos
        
        cols_needed = ['V01042', 'V01043', 'V01044', 'V01046', 'V01049', 'V01051']
        if all(c in df_par.columns for c in cols_needed):
            df_par['total_conjuges'] = df_par['V01043'] + df_par['V01044']
            df_par['taxa_conjugalidade'] = df_par['total_conjuges'] / df_par['V01042']
            df_par['taxa_monoparental_proxy'] = df_par['V01046'] / df_par['V01042']
            df_par['taxa_multigeracional'] = (df_par['V01049'] + df_par['V01051']) / df_par['V01042']
            
            cols_merge = ['CD_SETOR', 'taxa_conjugalidade', 'taxa_monoparental_proxy', 'taxa_multigeracional']
            df_final_massive = df_final_massive.merge(df_par[cols_merge], on='CD_SETOR', how='left')
            print("Features de Parentesco calculadas.")
else:
    print("Arquivo de Parentesco não encontrado.")

# --- 4. DOMICÍLIOS UNIPESSOAIS ---
print("\n--- Integrando Domicílios Unipessoais ---")
path_dom1 = os.path.join(data_dir, 'Agregados_por_setores_caracteristicas_domicilio1_BR.parquet')
if os.path.exists(path_dom1):
    df_dom1 = pd.read_parquet(path_dom1)
    # Normalizar chave
    if 'CD_SETOR' not in df_dom1.columns:
        if 'CD_setor' in df_dom1.columns: df_dom1 = df_dom1.rename(columns={'CD_setor': 'CD_SETOR'})
        elif 'index' in df_dom1.columns: df_dom1 = df_dom1.rename(columns={'index': 'CD_SETOR'})
    
    if 'CD_SETOR' in df_dom1.columns:
        df_dom1['CD_SETOR'] = df_dom1['CD_SETOR'].astype(str)
        
        # Procurar V00017 (case insensitive)
        col_unipessoal = None
        for c in df_dom1.columns:
            if 'v00017' in c.lower():
                col_unipessoal = c
                break
        
        if col_unipessoal:
            df_final_massive = df_final_massive.merge(df_dom1[['CD_SETOR', col_unipessoal]], on='CD_SETOR', how='left')
            if 'total_de_domicilios_particulares_dppo_dppv_dppuo_dpio_v0003' in df_final_massive.columns:
                df_final_massive['pct_domicilios_unipessoais'] = df_final_massive[col_unipessoal] / df_final_massive['total_de_domicilios_particulares_dppo_dppv_dppuo_dpio_v0003']
                print("Feature % Domicílios Unipessoais calculada.")
            else:
                print("Denominador de domicílios não encontrado no dataset massivo.")

# --- SALVAMENTO ---
print(f"\nSalvando dataset final em: {output_path}")
df_final_massive.to_parquet(output_path, index=False)
print(f"Concluído. Shape final: {df_final_massive.shape}")
