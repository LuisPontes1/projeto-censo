import duckdb
import os
import time
from pathlib import Path

# ==============================================================================
# CONFIGURAÇÃO
# ==============================================================================
# Define caminhos relativos ao projeto
BASE_DIR = Path(__file__).parent.parent 
INPUT_DIR = BASE_DIR / "data" / "raw" / "cnefe"
OUTPUT_FILE = BASE_DIR / "data" / "processed" / "cnefe_2022_optimized.parquet"

# Garante que a pasta de saída existe
os.makedirs(OUTPUT_FILE.parent, exist_ok=True)

# Configuração de Memória (Deixe uma folga para o SO)
MEMORY_LIMIT = "24GB" 

def ingest_cnefe():
    print(f"🚀 Iniciando Ingestão do CNEFE com DuckDB...")
    print(f"📂 Input: {INPUT_DIR}")
    print(f"💾 Output: {OUTPUT_FILE}")
    
    start_time = time.time()
    
    # Conecta em memória (mas usa disco se estourar a RAM)
    con = duckdb.connect(database=':memory:')
    con.execute(f"SET memory_limit='{MEMORY_LIMIT}';")
    
    # 1. DEFINIÇÃO DA LEITURA E LIMPEZA
    # O DuckDB consegue ler todos os CSVs da pasta com o wildcard *.csv
    # Usamos read_csv_auto, mas forçamos tipos para garantir performance
    
    print("⏳ Lendo CSVs, Limpando e Convertendo (Isso pode levar alguns minutos)...")
    
    # Caminho para globbing (DuckDB precisa de string)
    input_glob = str(INPUT_DIR / "*.csv")
    output_path = str(OUTPUT_FILE)
    
    # A Query abaixo faz tudo em uma passada só:
    # 1. Lê os CSVs
    # 2. Seleciona só as colunas úteis (Geocodificação)
    # 3. Limpa o CEP (Remove traço)
    # 4. Ordena por CEP e Número (Otimização crucial para o Join depois)
    
    # NOTA: Usamos PARTITION_BY (COD_UF) para quebrar o arquivo gigante em vários menores.
    # Isso ajuda no Git LFS (evita arquivo de 15GB) e ajuda no Spark (Partition Pruning).
    
    query = f"""
    COPY (
        SELECT 
            "COD_UF",
            "COD_MUN",
            "NOM_MUNICIPIO" as municipio,
            "NOM_BAIRRO" as bairro,
            "NOM_LOGRADOURO" as logradouro,
            TRIM("NUM_ENDERECO") as numero,
            
            -- Limpeza do CEP (Remove não numéricos)
            regexp_replace("NUM_CEP", '[^0-9]', '', 'g') as cep,
            
            -- Garante que Lat/Lon sejam numéricos
            CAST(REPLACE("LATITUDE", ',', '.') AS DOUBLE) as latitude,
            CAST(REPLACE("LONGITUDE", ',', '.') AS DOUBLE) as longitude,
            
            "COD_SETOR" as id_setor_censitario
            
        FROM read_csv_auto('{input_glob}', header=True, filename=True)
        WHERE 
            "LATITUDE" IS NOT NULL 
            AND "LONGITUDE" IS NOT NULL
            AND "NUM_CEP" IS NOT NULL
        
        ORDER BY cep, numero
    ) TO '{output_path}' (FORMAT 'PARQUET', CODEC 'SNAPPY', PARTITION_BY (COD_UF));
    """
    
    try:
        con.execute(query)
        end_time = time.time()
        duration = end_time - start_time
        
        # Verifica tamanho final
        size_gb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024 * 1024)
        
        print(f"✅ Sucesso! Arquivo gerado em {duration:.2f} segundos.")
        print(f"📦 Tamanho Final: {size_gb:.2f} GB")
        print(f"💡 Próximo passo: Suba este arquivo .parquet para o Databricks/S3.")
        
    except Exception as e:
        print(f"❌ Erro durante o processamento: {e}")
        print("Dica: Verifique se o caminho dos CSVs está correto e se os arquivos não estão corrompidos.")

if __name__ == "__main__":
    # Verifica se o DuckDB está instalado
    try:
        import duckdb
        ingest_cnefe()
    except ImportError:
        print("❌ Biblioteca 'duckdb' não encontrada.")
        print("Instale rodando: pip install duckdb")
