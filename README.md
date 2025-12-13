# 🇧🇷 Censo 2022: Feature Engineering & Spatial Intelligence

Este projeto realiza o processamento massivo e enriquecimento dos dados do Censo Demográfico 2022 (IBGE), transformando dados brutos agregados por setor censitário em inteligência de mercado acionável.

## 🎯 Objetivo
Criar um dataset "Gold Standard" para modelagem de **Risco de Crédito** e **Liquidez Imobiliária**, indo além da renda e explorando vulnerabilidade social, demografia e infraestrutura.

## 🧠 Camada Expert (Destaques)
O dataset final contém **731 features**, com destaque para indicadores calculados via engenharia de features avançada:

*   **Vulnerabilidade Infantil**: % de crianças (0-9 anos) sem acesso a água, esgoto ou coleta de lixo adequados.
*   **Risco Social (Proxy)**: Taxa de mortalidade de jovens (homens 15-29 anos) como proxy de violência e desorganização social.
*   **Potencial de Consumo**: Índice heurístico combinando Renda per Capita e Densidade Demográfica.
*   **Diversidade**: Índice de Simpson para heterogeneidade racial do setor.
*   **Estrutura Familiar**: Proxies para mães solo, lares unipessoais e razão de dependência.

## 🛠️ Tech Stack
*   **Python**: Linguagem principal.
*   **DuckDB**: Motor OLAP para processamento de alta performance e joins complexos em memória.
*   **Pandas**: Manipulação e exploração de dados.
*   **Parquet**: Formato de armazenamento colunar otimizado.

## 📂 Estrutura
*   `notebooks/`: Análises exploratórias e prototipagem.
*   `src/`: Código fonte Python.
*   `data/`: Contém as camadas de dados.
    *   `data/gold/`: **Dataset Final** (Versionado via Git LFS).
    *   `data/silver/`: Dados intermediários (Ignorado no Git).
*   `DATA_DICTIONARY.md`: Dicionário completo de variáveis.

## 🚀 Como usar
1. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```
2. Para reprocessar os dados (se tiver os arquivos raw):
   ```bash
   python notebooks/process_census_duckdb.py
   ```
3. Para carregar o dataset final:
   ```python
   import pandas as pd
   df = pd.read_parquet('data/gold/censo_2022_features_final.parquet')
   ```

## Como usar
(Instruções futuras sobre como rodar a pipeline de ingestão)
