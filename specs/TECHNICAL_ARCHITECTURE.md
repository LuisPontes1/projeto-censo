# Arquitetura Técnica

## Stack de Tecnologias

### Core
- **Python**: Linguagem principal.
- **DuckDB**: Motor SQL OLAP in-process para processar grandes volumes de dados censitários sem carregar tudo em RAM.
- **Pandas / Geopandas**: Manipulação de dataframes e dados geoespaciais.

### Bibliotecas Chave
- `geobr`: Acesso às malhas digitais (setores censitários, municípios, estados).
- `requests` / `aiohttp`: Download de dados brutos do FTP/API do IBGE.
- `pyarrow`: Manipulação de arquivos Parquet.
- `scikit-learn` (Futuro): Para modelagem e criação de features avançadas.

## Estrutura de Dados (Pipeline)

### 1. Camada Bronze (Raw)
- Armazenamento dos arquivos originais baixados do IBGE (CSV, Excel, Shapefiles).
- Estrutura de pastas: `data/raw/{ano}/{uf}/`
- Metadados preservados.

### 2. Camada Silver (Trusted)
- Dados convertidos para **Parquet** (compressão Snappy ou Zstd).
- Limpeza básica: tipagem de colunas, tratamento de nulos, padronização de nomes de colunas.
- Geometrias convertidas para **GeoParquet** ou WKB dentro do DuckDB.
- Join inicial de identificadores (Código do Setor).

### 3. Camada Gold (Refined/Analytical)
- Datasets prontos para consumo.
- **Tabela Mestra de Setores**: Uma linha por setor censitário com todas as features calculadas.
- Colunas de geometria (Lat/Long centróide, Polígono).
- Features agregadas:
    - Demografia (População total, por sexo, idade).
    - Domicílios (Tipo, saneamento, energia).
    - Renda (se disponível nos agregados).

## Fluxo de Trabalho
1.  **Crawler/Downloader**: Script para varrer o FTP do IBGE (similar ao `censobr`) e baixar arquivos.
2.  **Ingestion**: Leitura dos arquivos brutos e carga no DuckDB.
3.  **Processing**:
    - Normalização de códigos IBGE.
    - Download das malhas via `geobr`.
    - Join espacial e tabular.
4.  **Feature Engineering**: Cálculo de densidades, proporções e índices.
5.  **Export**: Geração dos arquivos `.parquet` finais.
