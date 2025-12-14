# 🗺️ Plano de Evolução: Inteligência Espacial (Spatial Layer)

**Data Alvo:** 14/12/2025
**Responsável:** GitHub Copilot (Expert em GeoAnalytics)
**Status:** Planejado

---

## 1. O Conceito: "De Ilhas para Paisagens"
Atualmente, nosso dataset Gold (`censo_2022_features_final.parquet`) trata cada Setor Censitário como uma ilha isolada.
*   **Problema:** Um setor de renda média cercado por favelas tem um risco diferente de um setor de renda média cercado por condomínios de luxo. O modelo atual não "vê" o vizinho.
*   **Solução:** Criar features de **Suavização Radial** e **Contexto Espacial**. O valor de um setor passa a ser uma função dele mesmo + o entorno.

## 2. Nova Arquitetura de Dados
Não vamos alterar o arquivo Gold atual. Vamos criar uma nova camada (Platinum/Diamond) que herda os dados da Gold e adiciona a inteligência espacial.

*   **Input 1:** `data/gold/censo_2022_features_final.parquet` (Dados Tabulares)
*   **Input 2:** **Malha de Setores Censitários 2022 (IBGE)** (Dados Geométricos - `.shp`, `.gpkg` ou `.parquet`)
*   **Output:** `data/diamond/censo_2022_spatial_features.parquet`

## 3. Features Planejadas (Spatial Feature Engineering)

**Meta:** Criar entre 40 a 50 features de alta inteligência, divididas em 4 camadas estratégicas.

### 3.1. Camada de Contexto (Smoothing) - ~15 Features
*Objetivo: Capturar o "clima" da região, removendo ruídos locais.*
*Técnica: KNN (K-Nearest Neighbors) com K=5 e K=10.*

*   **Renda**: `spatial_smooth_income_k5`, `spatial_smooth_income_k10` (Renda média da vizinhança).
*   **Risco Social**: `spatial_smooth_youth_mortality_k5` (Violência regional).
*   **Educação**: `spatial_smooth_literacy_rate_k5` (Capital humano do entorno).
*   **Infraestrutura**: `spatial_smooth_sanitation_index_k5` (Qualidade urbana regional).
*   **Pobreza**: `spatial_smooth_poverty_rate_k5` (% de domicílios vulneráveis no entorno).

### 3.2. Camada de Fricção & Segregação (Lags) - ~10 Features
*Objetivo: Identificar contrastes, muros invisíveis e segregação.*
*Técnica: Spatial Lag Ratio (Valor Setor / Valor Vizinhos).*

*   **Desigualdade de Renda**: `spatial_lag_income_ratio`.
    *   *> 1.5*: "Ilha de Riqueza" (Condomínio em área pobre).
    *   *< 0.8*: "Enclave de Serviço" (Comunidade em área nobre).
*   **Contraste de Violência**: `spatial_lag_mortality_contrast` (Setor seguro em região violenta?).
*   **Índice de Segregação**: `spatial_segregation_index` (Variância da renda entre os vizinhos imediatos).

### 3.3. Camada Expert: Clusters Estatísticos (LISA) - ~5 Features
*Objetivo: Prova estatística de Hotspots/Coldspots.*
*Técnica: Local Moran's I (LISA).*

*   **Cluster de Renda**: `expert_spatial_cluster_income` (Categórico: 0=NS, 1=HH, 2=LL, 3=HL, 4=LH).
    *   *HH*: Zona Nobre Consolidada.
    *   *LL*: Zona de Vulnerabilidade Crítica.
*   **Cluster de Risco**: `expert_spatial_cluster_mortality` (Zonas de Paz vs Zonas de Guerra).

### 3.4. Camada Expert: Gravidade de Mercado (Gravity Models) - ~10 Features
*Objetivo: Mensurar o "Mercado Endereçável" (Liquidez).*
*Técnica: Buffers (Raios de 1km e 2km) com Soma.*

*   **Massa Salarial**: `expert_market_gravity_wage_mass_1km` (Dinheiro total circulando a pé).
*   **Densidade de Clientes**: `expert_market_gravity_pop_density_1km`.
*   **Público Jovem**: `expert_market_gravity_youth_pop_1km` (Demanda por studios/aluguel).
*   **Potencial de Mercado**: `expert_market_potential_score` (Massa Salarial / Área).

## 4. Viabilidade Técnica

## 4. Viabilidade Técnica

### DuckDB Spatial Extension
O DuckDB suporta nativamente operações geoespaciais de alta performance.
*   **Comando:** `INSTALL spatial; LOAD spatial;`
*   **Capacidade:** Pode fazer joins espaciais (`ST_Intersects`, `ST_DWithin`) em milhões de linhas em segundos, superando o Geopandas puro para volumes massivos.

### Geopandas & PySAL
Utilizaremos para a lógica de pesos espaciais (Matriz de Vizinhança W).
*   **LibPySAL**: Para criar a matriz de pesos (Queen/Rook contiguity).
*   **Geopandas**: Para manipulação visual e validação das geometrias.

## 5. Próximos Passos (Checklist para Amanhã)
1.  [ ] Baixar a Malha de Setores Censitários 2022 (Brasil ou Estado piloto).
2.  [ ] Instalar extensão `spatial` no DuckDB.
3.  [ ] Criar script `notebooks/process_spatial_features.py`.
4.  [ ] Implementar lógica de KNN (K-Nearest Neighbors) para suavização.
5.  [ ] Gerar dataset `diamond`.

---
**Nota:** Esta camada exigirá maior poder computacional, pois operações geométricas são mais custosas que operações tabulares simples.
