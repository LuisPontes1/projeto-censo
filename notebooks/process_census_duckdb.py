import duckdb
import os

# Configuração de Caminhos
data_dir = r'C:\projetos\projeto-censo\data\silver'
output_dir = r'C:\projetos\projeto-censo\data\gold'
os.makedirs(output_dir, exist_ok=True)

db_path = os.path.join(output_dir, 'censo_duckdb.db')
output_parquet = os.path.join(output_dir, 'censo_2022_features_final.parquet')

print("--- INICIANDO PROCESSAMENTO COM DUCKDB (Alta Performance) ---")

con = duckdb.connect(database=':memory:')

# Função auxiliar para encontrar nome da coluna pelo código
def get_col_name(con, table_name, code):
    try:
        df_schema = con.execute(f"DESCRIBE {table_name}").df()
        # Procurar coluna que termina com _code ou contem code (case insensitive)
        for col in df_schema['column_name']:
            if code.lower() in col.lower():
                return col
        return None
    except Exception as e:
        print(f"Erro ao descrever tabela {table_name}: {e}")
        return None

# 1. CARREGAR DATASET MASSIVO
print("Carregando dataset massivo...")
massive_path = os.path.join(data_dir, 'censo_2022_agregado_massivo.parquet')
con.execute(f"CREATE OR REPLACE VIEW massive AS SELECT * FROM read_parquet('{massive_path}')")

# 2. CARREGAR TABELAS AUXILIARES
print("Carregando tabelas auxiliares...")

# Óbitos
obitos_path = os.path.join(data_dir, 'Agregados_por_setores_obitos_BR.parquet')
if os.path.exists(obitos_path):
    con.execute(f"CREATE OR REPLACE VIEW obitos_raw AS SELECT * FROM read_parquet('{obitos_path}')")
    
    # Helper to get columns safely
    def get_obito_col(code):
        return get_col_name(con, 'obitos_raw', code)
    
    cols_needed = {
        'V01226': 'v01226', 'V01227': 'v01227', # Total M/F
        'V01228': 'v01228', 'V01239': 'v01239', # 0-4 M/F
        'V01231': 'v01231', 'V01232': 'v01232', 'V01233': 'v01233' # 15-29 M
    }
    
    select_parts = ["CD_SETOR"]
    for alias, code in cols_needed.items():
        col_name = get_obito_col(code)
        if col_name:
            select_parts.append(f'"{col_name}" as {alias}')
        else:
            select_parts.append(f"0 as {alias}")
            
    con.execute(f"CREATE OR REPLACE VIEW obitos AS SELECT {', '.join(select_parts)} FROM obitos_raw")
else:
    con.execute("CREATE OR REPLACE VIEW obitos AS SELECT '0' as CD_SETOR, 0 as V01226, 0 as V01227, 0 as V01228, 0 as V01239, 0 as V01231, 0 as V01232, 0 as V01233")

# Demografia (População Detalhada)
demog_path = os.path.join(data_dir, 'Agregados_por_setores_demografia_BR.parquet')
if os.path.exists(demog_path):
    con.execute(f"CREATE OR REPLACE VIEW demog_raw AS SELECT * FROM read_parquet('{demog_path}')")
    
    def get_demog_col(code):
        return get_col_name(con, 'demog_raw', code)
        
    cols_needed = {
        'V01031': 'v01031', # Total 0-4
        'V01012': 'v01012', 'V01013': 'v01013', 'V01014': 'v01014' # Male 15-29
    }
    
    select_parts = ["CD_SETOR"]
    for alias, code in cols_needed.items():
        col_name = get_demog_col(code)
        if col_name:
            select_parts.append(f'"{col_name}" as {alias}')
        else:
            select_parts.append(f"0 as {alias}")
            
    con.execute(f"CREATE OR REPLACE VIEW demografia AS SELECT {', '.join(select_parts)} FROM demog_raw")
else:
    con.execute("CREATE OR REPLACE VIEW demografia AS SELECT '0' as CD_SETOR, 0 as V01031, 0 as V01012, 0 as V01013, 0 as V01014")

# Parentesco
parentesco_path = os.path.join(data_dir, 'Agregados_por_setores_parentesco_BR.parquet')
if os.path.exists(parentesco_path):
    con.execute(f"CREATE OR REPLACE VIEW parentesco_raw AS SELECT * FROM read_parquet('{parentesco_path}')")
    cols_map = {
        'V01042': get_col_name(con, 'parentesco_raw', 'v01042'),
        'V01043': get_col_name(con, 'parentesco_raw', 'v01043'),
        'V01044': get_col_name(con, 'parentesco_raw', 'v01044'),
        'V01046': get_col_name(con, 'parentesco_raw', 'v01046'),
        'V01049': get_col_name(con, 'parentesco_raw', 'v01049'),
        'V01051': get_col_name(con, 'parentesco_raw', 'v01051')
    }
    select_parts = ["CD_SETOR"]
    for code, col_name in cols_map.items():
        if col_name: select_parts.append(f'"{col_name}" as {code}')
        else: select_parts.append(f"0 as {code}")
    con.execute(f"CREATE OR REPLACE VIEW parentesco AS SELECT {', '.join(select_parts)} FROM parentesco_raw")
else:
    con.execute("CREATE OR REPLACE VIEW parentesco AS SELECT '0' as CD_SETOR, 0 as V01042, 0 as V01043, 0 as V01044, 0 as V01046, 0 as V01049, 0 as V01051")

# Domicilio 1
dom1_path = os.path.join(data_dir, 'Agregados_por_setores_caracteristicas_domicilio1_BR.parquet')
if os.path.exists(dom1_path):
    con.execute(f"CREATE OR REPLACE VIEW dom1_raw AS SELECT * FROM read_parquet('{dom1_path}')")
    col_v00017 = get_col_name(con, 'dom1_raw', 'v00017')
    if col_v00017:
        con.execute(f'CREATE OR REPLACE VIEW dom1 AS SELECT CD_SETOR, "{col_v00017}" as v00017_unipessoal FROM dom1_raw')
    else:
        con.execute("CREATE OR REPLACE VIEW dom1 AS SELECT '0' as CD_SETOR, 0 as v00017_unipessoal")
else:
    con.execute("CREATE OR REPLACE VIEW dom1 AS SELECT '0' as CD_SETOR, 0 as v00017_unipessoal")

# Alfabetização
alfab_path = os.path.join(data_dir, 'Agregados_por_setores_alfabetizacao_BR.parquet')
if os.path.exists(alfab_path):
    con.execute(f"CREATE OR REPLACE VIEW alfab_raw AS SELECT * FROM read_parquet('{alfab_path}')")
    
    # Total 15+ (V00644 to V00656)
    total_cols = [get_col_name(con, 'alfab_raw', f'v00{i}') for i in range(644, 657)]
    # Literate 15+ (V00748 to V00760)
    lit_cols = [get_col_name(con, 'alfab_raw', f'v00{i}') for i in range(748, 761)]
    
    # Build Sum Query
    total_sum = " + ".join([f'COALESCE("{c}", 0)' for c in total_cols if c])
    lit_sum = " + ".join([f'COALESCE("{c}", 0)' for c in lit_cols if c])
    
    if total_sum and lit_sum:
        con.execute(f"""
            CREATE OR REPLACE VIEW alfabetizacao AS 
            SELECT CD_SETOR, ({total_sum}) as total_15_mais, ({lit_sum}) as alfabetizados_15_mais 
            FROM alfab_raw
        """)
    else:
        con.execute("CREATE OR REPLACE VIEW alfabetizacao AS SELECT '0' as CD_SETOR, 0 as total_15_mais, 0 as alfabetizados_15_mais")
else:
    con.execute("CREATE OR REPLACE VIEW alfabetizacao AS SELECT '0' as CD_SETOR, 0 as total_15_mais, 0 as alfabetizados_15_mais")

# Domicilio 3 (Crianças e Saneamento)
dom3_path = os.path.join(data_dir, 'Agregados_por_setores_caracteristicas_domicilio3_BR_20250417.parquet')
if os.path.exists(dom3_path):
    con.execute(f"CREATE OR REPLACE VIEW dom3_raw AS SELECT * FROM read_parquet('{dom3_path}')")
    
    # Helper to get list of columns
    def get_cols(start, end):
        return [get_col_name(con, 'dom3_raw', f'v00{i}') for i in range(start, end + 1)]

    # Water Children (516-523)
    water_cols = get_cols(516, 523)
    # Inadequate: 518-523 (Poço raso, Fonte, Carro-pipa, Chuva, Rios, Outra)
    water_inad_cols = get_cols(518, 523)
    
    # Sewage Children (588-595)
    sewage_cols = get_cols(588, 595)
    # Inadequate: 591-595 (Fossa rudimentar, Vala, Rio, Outra, Não tem)
    sewage_inad_cols = get_cols(591, 595)
    
    # Garbage Children (618-623)
    garbage_cols = get_cols(618, 623)
    # Inadequate: 620-623 (Queimado, Enterrado, Baldio, Outro)
    garbage_inad_cols = get_cols(620, 623)
    
    # Build Sum Queries (using quoted columns)
    def build_sum(cols):
        valid_cols = [c for c in cols if c]
        if not valid_cols: return "0"
        return " + ".join([f'COALESCE("{c}", 0)' for c in valid_cols])

    con.execute(f"""
        CREATE OR REPLACE VIEW dom3 AS
        SELECT 
            CAST(setor AS VARCHAR) as CD_SETOR,
            ({build_sum(water_cols)}) as total_criancas_agua,
            ({build_sum(water_inad_cols)}) as criancas_agua_inadequada,
            ({build_sum(sewage_cols)}) as total_criancas_esgoto,
            ({build_sum(sewage_inad_cols)}) as criancas_esgoto_inadequado,
            ({build_sum(garbage_cols)}) as total_criancas_lixo,
            ({build_sum(garbage_inad_cols)}) as criancas_lixo_inadequado
        FROM dom3_raw
    """)
else:
    con.execute("""
        CREATE OR REPLACE VIEW dom3 AS 
        SELECT '0' as CD_SETOR, 
        0 as total_criancas_agua, 0 as criancas_agua_inadequada,
        0 as total_criancas_esgoto, 0 as criancas_esgoto_inadequado,
        0 as total_criancas_lixo, 0 as criancas_lixo_inadequado
    """)


# 3. EXECUTAR JOIN E FEATURE ENGINEERING
print("Executando Feature Engineering via SQL...")

query = """
SELECT 
    m.*,
    
    -- --- CAMADA EXPERT: DEMOGRAFIA ---
    (COALESCE(m.pop_0_19, 0) + COALESCE(m.pop_70_plus, 0)) / NULLIF(m.pop_20_69, 0) AS expert_demog_dependency_ratio,
    COALESCE(m.pop_70_plus, 0) / NULLIF(m.pop_0_19, 0) AS expert_demog_aging_index,
    
    -- --- CAMADA EXPERT: DIVERSIDADE RACIAL ---
    CASE 
        WHEN m.pct_branca IS NOT NULL THEN
            1 - (
                POWER(m.pct_branca, 2) + 
                POWER(m.pct_preta, 2) + 
                POWER(m.pct_parda, 2) + 
                POWER(GREATEST(0, 1 - (m.pct_branca + m.pct_preta + m.pct_parda)), 2)
            )
        ELSE NULL 
    END AS expert_social_racial_diversity_index,
    
    -- --- CAMADA EXPERT: POTENCIAL DE CONSUMO ---
    CASE 
        WHEN m.renda_per_capita_sm IS NOT NULL AND m.densidade_demografica IS NOT NULL THEN
            m.renda_per_capita_sm * LN(m.densidade_demografica + 1)
        ELSE NULL
    END AS expert_econ_consumption_potential_index,
    
    -- --- CAMADA EXPERT: MORTALIDADE ---
    (COALESCE(o.V01226, 0) + COALESCE(o.V01227, 0)) AS raw_health_total_deaths,
    CASE 
        WHEN m.total_de_pessoas_v0001 > 0 THEN
            ((COALESCE(o.V01226, 0) + COALESCE(o.V01227, 0)) / m.total_de_pessoas_v0001) * 1000
        ELSE 0
    END AS expert_health_mortality_rate_1000,

    -- Youth Mortality (Male 15-29)
    (COALESCE(o.V01231, 0) + COALESCE(o.V01232, 0) + COALESCE(o.V01233, 0)) AS raw_health_youth_male_deaths,
    (COALESCE(dem.V01012, 0) + COALESCE(dem.V01013, 0) + COALESCE(dem.V01014, 0)) AS raw_demog_youth_male_pop,
    CASE 
        WHEN (COALESCE(dem.V01012, 0) + COALESCE(dem.V01013, 0) + COALESCE(dem.V01014, 0)) > 0 THEN
            ((COALESCE(o.V01231, 0) + COALESCE(o.V01232, 0) + COALESCE(o.V01233, 0)) / 
             (COALESCE(dem.V01012, 0) + COALESCE(dem.V01013, 0) + COALESCE(dem.V01014, 0))) * 1000
        ELSE 0
    END AS expert_health_youth_mortality_rate,
    
    -- --- CAMADA EXPERT: ESTRUTURA FAMILIAR ---
    CASE 
        WHEN p.V01042 > 0 THEN
            (COALESCE(p.V01043, 0) + COALESCE(p.V01044, 0)) / p.V01042
        ELSE 0
    END AS expert_family_conjugality_rate,
    
    CASE 
        WHEN p.V01042 > 0 THEN
            COALESCE(p.V01046, 0) / p.V01042
        ELSE 0
    END AS expert_family_single_parent_proxy,
    
    CASE 
        WHEN p.V01042 > 0 THEN
            (COALESCE(p.V01049, 0) + COALESCE(p.V01051, 0)) / p.V01042
        ELSE 0
    END AS expert_family_multigenerational_rate,
    
    -- --- CAMADA EXPERT: DOMICÍLIOS UNIPESSOAIS ---
    CASE 
        WHEN m.total_de_domicilios_particulares_dppo_dppv_dppuo_dpio_v0003 > 0 THEN
            COALESCE(d1.v00017_unipessoal, 0) / m.total_de_domicilios_particulares_dppo_dppv_dppuo_dpio_v0003
        ELSE 0
    END AS expert_family_one_person_household_pct,
    
    -- --- CAMADA EXPERT: EDUCAÇÃO (ALFABETIZAÇÃO) ---
    CASE 
        WHEN a.total_15_mais > 0 THEN
            LEAST(1.0, COALESCE(a.alfabetizados_15_mais, 0) / a.total_15_mais)
        ELSE NULL
    END AS expert_edu_literacy_rate_15_plus,

    -- --- CAMADA EXPERT: VULNERABILIDADE INFANTIL (SANEAMENTO) ---
    CASE 
        WHEN d3.total_criancas_agua > 0 THEN 
            d3.criancas_agua_inadequada / d3.total_criancas_agua 
        ELSE 0 
    END as expert_child_vuln_water_inadequate_pct,
    
    CASE 
        WHEN d3.total_criancas_esgoto > 0 THEN 
            d3.criancas_esgoto_inadequado / d3.total_criancas_esgoto 
        ELSE 0 
    END as expert_child_vuln_sewage_inadequate_pct,
    
    CASE 
        WHEN d3.total_criancas_lixo > 0 THEN 
            d3.criancas_lixo_inadequado / d3.total_criancas_lixo 
        ELSE 0 
    END as expert_child_vuln_garbage_inadequate_pct

FROM massive m
LEFT JOIN obitos o ON m.CD_SETOR = o.CD_SETOR
LEFT JOIN demografia dem ON m.CD_SETOR = dem.CD_SETOR
LEFT JOIN parentesco p ON m.CD_SETOR = p.CD_SETOR
LEFT JOIN dom1 d1 ON m.CD_SETOR = d1.CD_SETOR
LEFT JOIN alfabetizacao a ON m.CD_SETOR = a.CD_SETOR
LEFT JOIN dom3 d3 ON m.CD_SETOR = d3.CD_SETOR
"""

print("Executando query e salvando em Parquet...")
con.execute(f"COPY ({query}) TO '{output_parquet}' (FORMAT PARQUET)")

print(f"\nSUCESSO! Arquivo salvo em: {output_parquet}")
result_shape = con.execute(f"SELECT COUNT(*) FROM read_parquet('{output_parquet}')").fetchone()
print(f"Total de linhas no dataset final: {result_shape[0]}")

con.close()
