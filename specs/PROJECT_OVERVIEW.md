# Projeto Censo 2022 - Geo-Analytics Platform

## Visão Geral
Este projeto visa criar uma pipeline completa de engenharia de dados e análise geoespacial para o Censo Demográfico 2022 do Brasil. O objetivo é ingerir, tratar, enriquecer e disponibilizar microdados censitários integrados com malhas geográficas (setores censitários) para modelagem e análise avançada.

## Objetivos Principais
1.  **Ingestão de Dados**: Automatizar o download e processamento dos microdados do Censo 2022 (IBGE).
2.  **Integração Geoespacial**: Vincular dados tabulares aos setores censitários (geometrias) usando `geobr` e fontes oficiais.
3.  **Engenharia de Features**: Criar variáveis derivadas, agregações e índices para facilitar modelagem (ex: densidade demográfica, renda média por setor, etc.).
4.  **Persistência Otimizada**: Armazenar dados em formato `Parquet` e `DuckDB` para consultas analíticas de alta performance.
5.  **Dataset Final**: Disponibilizar um "Gold Dataset" pronto para uso em Machine Learning e Geofísica.

## Referências
- **censobr** (R package): Inspiração para a lógica de download e estruturação dos dados censitários. Como não possui versão Python nativa, implementaremos a lógica de ingestão equivalente.
- **geobr** (Python package): Biblioteca oficial para obtenção das malhas territoriais do Brasil.
- **IBGE**: Fonte primária dos dados.

## Stack Tecnológico
- **Linguagem**: Python 3.10+
- **Geoprocessamento**: Geopandas, Shapely, PyGEOS
- **Processamento de Dados**: Pandas, Polars (opcional para performance), DuckDB
- **Armazenamento**: Parquet, GeoParquet
- **Orquestração**: Scripts Python modulares (potencial para Airflow/Prefect futuro)
