# Census 2022 Feature Dictionary

This document details the variables available in the `censo_2022_features_final.parquet` dataset. 

**Total Columns:** 731
**Focus:** This documentation highlights the **Expert** and **Calculated** layers. The dataset **preserves all ~700 raw variables** from the IBGE Aggregates (Basico, Domicilio01, Domicilio02, Pessoa01, Pessoa03, Responsavel01, etc.) for deep-dive analysis.

The features are categorized by their level of processing and intelligence.

## Taxonomy
*   **`raw_`**: Direct columns from the source (IBGE), minimally processed. (Includes hundreds of `V00xx` columns).
*   **`calc_`**: Simple calculated indicators (percentages, densities) derived directly from raw data.
*   **`expert_`**: Complex indicators, indices, or domain-specific logic derived from multiple sources or advanced formulas.

---

## 🧠 Expert Features (Detailed Documentation)

These features represent the "Expert Layer" of the dataset, providing high-value insights derived from complex logic.

### 👶 Child Vulnerability (0-9 Years)
These indicators specifically measure the quality of infrastructure for children aged 0 to 9 years, providing a direct view of early childhood vulnerability.

*   **`expert_child_vuln_water_inadequate_pct`**
    *   **Description**: Percentage of children (0-9 years) living in households with inadequate water supply.
    *   **Calculation**: `(Children in households with inadequate water) / (Total children in households with water info)`
    *   **Inadequate Definition**: Water from shallow wells, springs, tanker trucks, rain, rivers, or other sources (excluding general distribution network and deep wells).
    *   **Source**: Domicilio03 (V0518-V0523).

*   **`expert_child_vuln_sewage_inadequate_pct`**
    *   **Description**: Percentage of children (0-9 years) living in households with inadequate sewage disposal.
    *   **Calculation**: `(Children in households with inadequate sewage) / (Total children in households with sewage info)`
    *   **Inadequate Definition**: Rudimentary septic tanks, ditches, rivers/lakes, or no bathroom/toilet.
    *   **Source**: Domicilio03 (V0591-V0595).

*   **`expert_child_vuln_garbage_inadequate_pct`**
    *   **Description**: Percentage of children (0-9 years) living in households with inadequate garbage disposal.
    *   **Calculation**: `(Children in households with inadequate garbage) / (Total children in households with garbage info)`
    *   **Inadequate Definition**: Burnt, buried on property, thrown in vacant lots/public areas, or other destinations (excluding collection service or skip).
    *   **Source**: Domicilio03 (V0620-V0623).

### 📊 Demography & Society

*   **`expert_social_racial_diversity_index`**
    *   **Description**: A measure of racial heterogeneity within the census sector (Simpson's Index of Diversity).
    *   **Range**: 0 (No diversity, everyone is the same race) to ~0.75 (Maximum diversity).
    *   **Calculation**: $1 - \sum (p_i^2)$, where $p_i$ is the proportion of each racial group (White, Black, Brown, Other).
    *   **Insight**: High values indicate mixed neighborhoods; low values indicate racially segregated or homogenous areas.

*   **`expert_demog_dependency_ratio`**
    *   **Description**: The ratio of dependents (young <20 and elderly >70) to the working-age population (20-69).
    *   **Calculation**: `(Pop 0-19 + Pop 70+) / (Pop 20-69)`
    *   **Insight**: Higher values indicate a higher economic burden on the working population.

*   **`expert_demog_aging_index`**
    *   **Description**: The number of elderly people (70+) per child/youth (0-19).
    *   **Calculation**: `Pop 70+ / Pop 0-19`
    *   **Insight**: Indicates the stage of demographic transition. Values > 1 indicate an aged population.

### 💰 Economy

*   **`expert_econ_consumption_potential_index`**
    *   **Description**: A heuristic index estimating the market potential of an area.
    *   **Calculation**: `Income Per Capita (SM) * ln(Population Density + 1)`
    *   **Insight**: Combines purchasing power with consumer density. High values indicate prime areas for retail/services (high density + high income).

### 🏥 Health

*   **`expert_health_youth_mortality_rate`** (New!)
    *   **Description**: Mortality rate specifically for young males (15-29 years old) per 1,000 young males.
    *   **Calculation**: `(Deaths Male 15-29 / Population Male 15-29) * 1000`
    *   **Insight**: This is a critical proxy for **external causes of death** (violence, accidents) and social risk. High values in this specific demographic often correlate with higher community violence levels.
    *   **Source**: Mortality File (V01231-V01233) + Demography File (V01012-V01014).

*   **`expert_health_mortality_rate_1000`**
    *   **Description**: The crude mortality rate per 1,000 inhabitants (all ages).
    *   **Calculation**: `(Total Deaths / Total Population) * 1000`
    *   **Source**: Mortality File (Obitos) + Basic File (Basico).

### 🏠 Family Structure

*   **`expert_family_conjugality_rate`**
    *   **Description**: Proportion of household heads living with a spouse or partner.
    *   **Calculation**: `(Spouses + Partners) / Total Heads of Household`

*   **`expert_family_single_parent_proxy`**
    *   **Description**: Proxy for single-parent households (Heads without spouse, with children).
    *   **Calculation**: `(Heads without spouse with children) / Total Heads of Household`

*   **`expert_family_multigenerational_rate`**
    *   **Description**: Proportion of households containing extended family (grandchildren, parents, grandparents).
    *   **Calculation**: `(Grandchildren + Parents/Grandparents) / Total Heads of Household`

*   **`expert_family_one_person_household_pct`**
    *   **Description**: Percentage of households occupied by a single person.
    *   **Calculation**: `One-person households / Total permanent private households`

### 🎓 Education

*   **`expert_edu_literacy_rate_15_plus`**
    *   **Description**: Literacy rate for the population aged 15 and older.
    *   **Calculation**: `Literate (15+) / Total Population (15+)`

---

## ⚙️ Calculated Features (`calc_`)

These are standard derived metrics useful for general analysis.

*   **`calc_pop_density`** (`densidade_demografica`): Inhabitants per km².
*   **`calc_sex_ratio`** (`razao_sexo`): Males per 100 Females.
*   **`calc_avg_income_sm`** (`rendimento_medio_responsavel_sm`): Average income of household heads in Minimum Wages.
*   **`calc_income_per_capita_sm`** (`renda_per_capita_sm`): Estimated per capita income in Minimum Wages.
*   **`calc_income_inequality_cv`** (`cv_renda_desigualdade`): Coefficient of Variation of income (proxy for inequality).
*   **`calc_pct_white`** (`pct_branca`): % of population identifying as White.
*   **`calc_pct_black`** (`pct_preta`): % of population identifying as Black.
*   **`calc_pct_brown`** (`pct_parda`): % of population identifying as Brown (Parda).

---

## 🔢 Contextual Quantities (`raw_`)

These are the absolute counts used as denominators or numerators for the expert features. They are included to provide context on the *magnitude* of the phenomena (e.g., a 100% rate in a sector with only 1 person is different from a 100% rate in a sector with 1000 people).

### Population & Demography
*   **`raw_demog_youth_male_pop`**: Total male population aged 15-29.
*   **`raw_pop_0_19`**: Total population aged 0-19.
*   **`raw_pop_20_69`**: Total population aged 20-69.
*   **`raw_pop_70_plus`**: Total population aged 70+.

### Health (Deaths)
*   **`raw_health_total_deaths`**: Total deaths recorded in the sector (12 months prior to census).
*   **`raw_health_youth_male_deaths`**: Deaths of males aged 15-29.

### Child Vulnerability (Counts)
*   **`total_criancas_agua`**: Total children (0-9) in households with water supply info.
*   **`criancas_agua_inadequada`**: Count of children (0-9) with inadequate water supply.
*   **`total_criancas_esgoto`**: Total children (0-9) in households with sewage info.
*   **`criancas_esgoto_inadequado`**: Count of children (0-9) with inadequate sewage.
*   **`total_criancas_lixo`**: Total children (0-9) in households with garbage info.
*   **`criancas_lixo_inadequado`**: Count of children (0-9) with inadequate garbage disposal.

---

## 📄 Raw Features (`raw_`)

Direct identifiers and counts from IBGE.

*   **`CD_SETOR`**: Unique 15-digit Census Sector ID.
*   **`NM_MUN`**: Municipality Name.
*   **`NM_UF`**: State Name.
*   **`total_de_pessoas_v0001`**: Total population count.
*   **`total_de_domicilios...`**: Total household count.
*   **`AREA_KM2`**: Sector area in km².

---

## 📈 Summary of Indicators

| Category | Prefix | Count | Description |
| :--- | :--- | :---: | :--- |
| **Expert Indicators** | `expert_` | **14** | High-value, complex derived metrics (Risk, Vulnerability, Potential). |
| **Calculated Metrics** | `calc_` | **8** | Standard densities, ratios, and percentages. |
| **Contextual Quantities** | `raw_` | **12** | Key absolute counts for validation and context. |
| **Raw Census Variables** | `V...` | **~710** | **All original IBGE variables** preserved for deep-dive analysis. |
| **Identifiers & Metadata** | `raw_` | **6** | Keys, names, and area. |
| **Total Features** | | **731** | Total available columns in the final dataset. |

### Detailed Breakdown by Category

#### 🧠 Expert Indicators (14)
*   **Family Structure (4)**: Conjugality, Single Parent Proxy, Multigenerational, One-Person Households.
*   **Child Vulnerability (3)**: Water, Sewage, Garbage inadequacy (0-9 years).
*   **Demography & Society (3)**: Racial Diversity, Dependency Ratio, Aging Index.
*   **Health (2)**: General Mortality, Youth Mortality (Risk Proxy).
*   **Economy (1)**: Consumption Potential.
*   **Education (1)**: Literacy Rate (15+).

#### ⚙️ Calculated Metrics (8)
*   **Demography**: Population Density, Sex Ratio.
*   **Economy**: Avg Income, Per Capita Income, Inequality (CV).
*   **Race**: % White, % Black, % Brown.

#### 🔢 Contextual Quantities (12)
*   **Child Vulnerability Counts**: 6 variables (Totals and Inadequate counts).
*   **Population Segments**: 4 variables (Youth Male, 0-19, 20-69, 70+).
*   **Health Counts**: 2 variables (Total Deaths, Youth Male Deaths).

#### 📄 Raw Census Variables (~700)
*   **Households (Domicílios)**: **476 variables**. Detailed infrastructure (water, sewage, garbage), household goods, and living conditions.
*   **Population (Pessoas)**: **187 variables**. Detailed age groups, sex, race, and indigenous/quilombola status.
*   **Surroundings (Entorno)**: **35 variables**. Characteristics of the street/sector (paving, lighting, sidewalks).
*   **Income (Renda)**: **9 variables**. Average income, per capita income, wage mass, and inequality metrics.
*   **Metadata**: **7 variables**. Sector IDs, names, and area.

#### 📄 Identifiers & Metadata (6)
*   **Keys**: Sector ID, Municipality, State.
*   **Totals**: Total Population, Total Households, Area (km²).


