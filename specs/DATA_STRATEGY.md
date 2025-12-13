# Estratégia de Dados e Ingestão

## Fontes de Dados

### 1. Dados Censitários (Alfanuméricos)
Como o pacote `censobr` é exclusivo para R, desenvolveremos um módulo Python `ibge_downloader` que replica sua funcionalidade:
- **Fonte**: FTP do IBGE ou API de Dados Agregados (SIDRA) para dados preliminares do Censo 2022.
- **Microdados**: Se disponíveis, baixar os arquivos de microdados (geralmente grandes CSVs ou arquivos de largura fixa).
- **Agregados por Setor**: O foco inicial será nos "Agregados por Setores Censitários" (Resultados do Universo), que permitem a granularidade máxima geográfica.

### 2. Dados Geográficos (Vetoriais)
Utilizaremos a biblioteca `geobr` para Python.
- **Função Principal**: `read_census_tract(code_muni='all', year=2022)` (verificar disponibilidade de 2022, caso contrário usar 2010 e de-para ou aguardar release).
- **Fallback**: Download direto dos Shapefiles/GeoPackages do site do IBGE se o `geobr` não tiver a malha 2022 atualizada.

## Estratégia de Cruzamento (Join)
O ponto crucial é o **Código do Setor Censitário** (CD_SETOR).
1.  Garantir que o código do setor nos dados alfanuméricos esteja no mesmo formato (string vs int, zeros à esquerda) que nos dados vetoriais.
2.  Realizar o join: `Geometria (Setor) <-> Dados (Setor)`.

## Definição das Features (Exemplos)
O dataset final deverá conter:
- `setor_id`: Chave primária.
- `geometry`: Polígono do setor.
- `centroide_lat`, `centroide_lon`: Coordenadas do centro.
- `populacao_total`: Contagem.
- `domicilios_particulares`: Contagem.
- `media_moradores_domicilio`: Feature calculada.
- `densidade_demografica`: `populacao / area_km2`.

## Armazenamento
- Utilizar **DuckDB** como "Lakehouse" local.
- Tabelas particionadas por UF (Unidade Federativa) para otimizar leitura.
