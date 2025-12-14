# Guia de Ingestão do CNEFE (Geocodificação)

Este módulo contém os scripts para baixar e processar o **Cadastro Nacional de Endereços para Fins Estatísticos (CNEFE)** do IBGE, transformando-o em uma base Parquet otimizada para geocodificação em alta performance.

## Estrutura de Pastas
Os scripts utilizam caminhos relativos. A estrutura esperada/gerada é:

```
projeto-censo/
├── src/
│   ├── download_cnefe_ftp.py    # Baixa os ZIPs do IBGE
│   └── ingest_cnefe_to_parquet.py # Converte CSV -> Parquet Otimizado
├── data/
│   ├── raw/
│   │   └── cnefe/               # Onde os CSVs brutos são salvos
│   └── processed/
│       └── cnefe_2022_optimized.parquet # O arquivo final (Ouro)
```

## Como Executar

### Opção A: Rodar Localmente (Seu PC)
1. Instale as dependências: `pip install duckdb`
2. Execute o download: `python src/download_cnefe_ftp.py`
3. Execute o processamento: `python src/ingest_cnefe_to_parquet.py`

### Opção B: Rodar no Databricks (Stone)
Como o arquivo final é muito grande (>10GB) para o GitHub, a melhor estratégia é **levar o código até o dado**, e não o dado até o código.

1. Clone este repositório no Databricks.
2. Execute o script `src/download_cnefe_ftp.py` no cluster (ele baixará do IBGE direto para o DBFS/Driver).
3. Execute o script `src/ingest_cnefe_to_parquet.py`.
4. O resultado estará pronto para uso nos seus Jobs de Spark.

## Detalhes Técnicos
- **Fonte**: FTP do IBGE (Censo 2022).
- **Formato Final**: Parquet com compressão Snappy.
- **Otimização**: Os dados são ordenados fisicamente por CEP e Número para permitir "Range Scans" ultra-rápidos.
