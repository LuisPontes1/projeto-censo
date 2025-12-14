# 💎 Censo 2022 Diamond Dataset: Technical Documentation

**Prepared for:** Stone Co.  
**Attention:** Russell Weiss & Luis Felipe Rejani Pontes  
**Version:** 2.0 (Premium Edition)  
**Date:** December 2025  
**Format:** Parquet (GeoPandas/PyArrow optimized)  
**Coverage:** Brazil (National) - Census Tract Level

---

## 1. Executive Summary

The **Diamond Dataset** is the most advanced socio-economic intelligence layer available for the 2022 Census. Unlike standard raw data, this dataset applies **spatial data science**, **gravity models**, and **unsupervised learning** to reveal hidden patterns in the urban fabric.

It transforms static census counts into dynamic indicators of **Risk**, **Opportunity**, **Isolation**, and **Influence**.

### Key Differentiators
*   **Spatial Context Awareness**: Every metric "knows" its neighbors. We distinguish between a rich area surrounded by poverty (High Inequality) vs. a rich area surrounded by wealth (High Cluster).
*   **Gravity Models**: We calculated the "gravitational pull" of wealth and population for every single census tract using Hansen's Accessibility Model.
*   **Anomaly Detection**: Built-in Z-scores and Isolation Indices instantly flag outliers (e.g., pockets of extreme poverty in wealthy districts).

---

## 2. Data Architecture

The dataset is split into two optimized files to ensure performance and flexibility:

1.  **`malha_setores_2022_final.parquet` (The Mesh)**
    *   **Content**: Geometry (Polygons) and Registry Data (IDs, Names).
    *   **Use Case**: Mapping, Spatial Joins, Visualization.
    *   **Columns**: `id_setor`, `geometry`, `nm_distrito`, `nm_bairro`, `nm_subdistrito`.

2.  **`censo_2022_diamond_features_final.parquet` (The Intelligence)**
    *   **Content**: ~1,100 Numeric Features (Raw + Calculated + Expert + Spatial).
    *   **Use Case**: Machine Learning, Statistical Analysis, Dashboarding.
    *   **Key**: `id_setor` (String).
    *   **Relationship**: This file contains **ALL** metrics from the Gold dataset, plus the new Spatial and Gravity layers.

---

## 3. Variable Summary by Theme

| Theme | Count | Description |
| :--- | :--- | :--- |
| **Expert (Intelligence)** | **308** | Complex indices (Diversity, Vulnerability, Potential) + Spatial derivatives. |
| **Spatial (Lag/LISA)** | **196** | Neighborhood statistics (Lag, Inequality, Isolation, Clusters). |
| **Gravity (Accessibility)** | **9** | Hansen's Accessibility Indices (1km, 2km, 5km). |
| **Income (Renda)** | **33** | Average income, wage mass, inequality metrics. |
| **Demography (Pop/Age)** | **142** | Population counts, age pyramids, dependency ratios. |
| **Households (Domicílios)** | **500** | Infrastructure, goods, living conditions. |
| **Raw (V00xx)** | **733** | Original IBGE variables preserved for deep-dive. |
| **Total Unique Columns** | **1,097** | *Note: Some columns overlap categories.* |

---

## 4. Methodology & Algorithms

### 4.1. The "Expert" Layer
Domain-specific indicators derived from complex combinations of raw variables.

| Indicator | Description | Business Value |
| :--- | :--- | :--- |
| **`expert_econ_consumption_potential_index`** | `Income * ln(Density)` | Identifies prime retail locations (High Density + High Income). |
| **`expert_social_racial_diversity_index`** | Simpson's Diversity Index ($1 - \sum p_i^2$) | Measures racial heterogeneity. 0=Segregated, 0.75=Diverse. |
| **`expert_demog_aging_index`** | `Pop 70+ / Pop 0-19` | Critical for healthcare and senior living planning. |
| **`expert_health_youth_mortality_rate`** | Deaths (Male 15-29) / Pop | **Strong proxy for violence and social risk.** |
| **`expert_child_vuln_...`** | Water/Sewage/Garbage inadequacy | Flags areas needing immediate infrastructure investment. |

### 4.2. The "Spatial" Layer (K-Nearest Neighbors)
For every Expert variable, we computed spatial statistics using **K=5, 10, and 15** nearest neighbors.

#### **Suffix Definitions:**
*   **`_lag_k5` (Neighborhood Context)**: The average value of the 5 nearest neighbors.
    *   *Interpretation*: "Tell me who your neighbors are, and I'll tell you who you are."
*   **`_inequality_k5` (Local Inequality)**: The Coefficient of Variation (CV) of the neighborhood.
    *   *Formula*: $\sigma_{neighbors} / \mu_{neighbors}$
    *   *Interpretation*: High values indicate a "fractured" neighborhood with mixed social classes.
*   **`_isolation_k5` (Anomaly Score)**: The absolute difference between the sector and its neighbors.
    *   *Formula*: $|Value_{self} - Value_{lag}|$
    *   *Interpretation*: Identifies outliers. A slum in a rich district (or vice versa) will have high isolation.
*   **`_rank_k5` (Local Z-Score)**: How many standard deviations the sector is from its local mean.
    *   *Interpretation*: Normalized ranking within the immediate vicinity.

### 4.3. The "Gravity" Layer (Hansen Accessibility)
We applied a physics-based Gravity Model ($Mass / Distance^\beta$) with $\beta=1.5$ to measure accessibility to wealth and population.

*   **`gravity_massa_salarial_total_1000m`**: The sum of all wage mass within 1km, decayed by distance.
    *   *Value*: Represents the "Economic Heat" of the location.
*   **`gravity_total_de_pessoas_v0001_2000m`**: The "Population Pressure" within 2km.
    *   *Value*: Useful for estimating footfall and service demand.
*   **Radii Available**: 1000m, 2000m, 5000m.

### 4.4. LISA Clusters (Local Indicators of Spatial Association)
We computed Local Moran's I to classify every sector into one of four quadrants:

*   **Cluster 1 (HH - High-High)**: Hotspots. High value surrounded by high values. (e.g., Deep Wealth).
*   **Cluster 2 (LH - Low-High)**: Cold Outliers. Low value surrounded by high values. (e.g., A favela in a rich district).
*   **Cluster 3 (LL - Low-Low)**: Coldspots. Low value surrounded by low values. (e.g., Deep Poverty).
*   **Cluster 4 (HL - High-Low)**: Hot Outliers. High value surrounded by low values. (e.g., A gated community in a poor area).

---

## 5. Detailed Expert Variable Dictionary

Below is the complete list of Expert variables available in the dataset, organized by business domain.

### 👶 Child Vulnerability (0-9 Years)
*Indicators focusing on the living conditions of the next generation.*

| Indicator | Description | Business Value (ESG & Public Policy) |
| :--- | :--- | :--- |
| **`expert_child_vuln_garbage_inadequate_pct`** | % Children in households with inadequate garbage disposal (burnt, buried). | Identifies areas with high environmental contamination risk and public health hazards. |
| **`expert_child_vuln_sewage_inadequate_pct`** | % Children in households with inadequate sewage (ditches, open air). | Critical for sanitation infrastructure planning and disease outbreak risk assessment. |
| **`expert_child_vuln_water_inadequate_pct`** | % Children in households with inadequate water supply (wells, rain). | Flags regions with water security issues; priority for social responsibility initiatives. |

### 📊 Demography & Society
*Indicators characterizing the population structure and diversity.*

| Indicator | Description | Business Value (Market Strategy) |
| :--- | :--- | :--- |
| **`expert_demog_aging_index`** | Ratio of elderly (70+) to youth (0-19). | **Silver Economy**: Essential for siting pharmacies, clinics, and senior living facilities. |
| **`expert_demog_dependency_ratio`** | Ratio of dependents (<20 & >70) to working age (20-69). | Measures economic burden on families; indicates disposable income constraints. |
| **`expert_social_racial_diversity_index`** | Racial heterogeneity index (0-1). | **DEI & Marketing**: Identifies cosmopolitan/diverse areas vs. segregated bubbles. |

### 💰 Economy & Education
*Indicators of purchasing power and human capital.*

| Indicator | Description | Business Value (Expansion & Credit) |
| :--- | :--- | :--- |
| **`expert_econ_consumption_potential_index`** | Composite index: `Income * ln(Density)`. | **The "Golden Metric" for Retail**: Pinpoints areas with high money flow AND high customer density. |
| **`expert_edu_literacy_rate_15_plus`** | Literacy rate for population 15+. | Proxy for workforce qualification and sophistication of consumer demand. |

### 🏠 Family Structure
*Indicators of household composition and lifestyle.*

| Indicator | Description | Business Value (Product Fit) |
| :--- | :--- | :--- |
| **`expert_family_conjugality_rate`** | % of household heads living with a spouse. | Targets for family-oriented products, insurance, and real estate. |
| **`expert_family_multigenerational_rate`** | % households with extended family (grandparents/grandchildren). | Indicates larger household sizes and shared wallet behavior. |
| **`expert_family_one_person_household_pct`** | % of single-person households. | **Solo Economy**: Targets for delivery apps, compact housing, and convenience services. |
| **`expert_family_single_parent_proxy`** | % of single parents (Head without spouse + children). | Identifies price-sensitive demographics and need for support services. |

### 🏥 Health & Risk
*Indicators of mortality and social violence.*

| Indicator | Description | Business Value (Risk & Insurance) |
| :--- | :--- | :--- |
| **`expert_health_mortality_rate_1000`** | General mortality rate per 1000 inhabitants. | Baseline for actuarial models and life insurance pricing. |
| **`expert_health_youth_mortality_rate`** | Mortality rate for young males (15-29). | **Security Proxy**: Highly correlated with urban violence and organized crime presence. |

---

## 6. Usage Guide

### How to identify "Investment Opportunities"?
Look for **High Consumption Potential** (`expert_econ_consumption_potential_index`) combined with **Low Competition/Density** (check `geo_avg_dist_k5`).

### How to identify "Social Risk"?
Filter for **Cluster 1 (HH)** in `expert_health_youth_mortality_rate_lisa_q_k5` OR High **Inequality** (`_inequality_k5`) in Income.

### How to find "Gentrification Frontiers"?
Look for sectors with **High Potential** (`HL` or `LH` clusters) where the neighborhood lag is changing.

---

*Confidential - Internal Use Only*
