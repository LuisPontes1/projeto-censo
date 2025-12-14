import ftplib
import os
import zipfile
import time
from pathlib import Path

# ==============================================================================
# CONFIGURAÇÃO
# ==============================================================================
FTP_HOST = 'ftp.ibge.gov.br'
# Caminho CORRETO com os endereços completos (Logradouro, Número, etc.)
FTP_PATH = '/Cadastro_Nacional_de_Enderecos_para_Fins_Estatisticos/Censo_Demografico_2022/Arquivos_CNEFE/CSV/UF/'

# Define caminhos relativos ao projeto (funciona no Windows e Linux/Databricks)
# Se o script roda em src/, o projeto está um nível acima
BASE_DIR = Path(__file__).parent.parent 
LOCAL_DIR = BASE_DIR / "data" / "raw" / "cnefe"

def download_and_extract():
    if not os.path.exists(LOCAL_DIR):
        os.makedirs(LOCAL_DIR)
        
    print(f"🚀 Conectando ao FTP do IBGE: {FTP_HOST}")
    print(f"📂 Salvando em: {LOCAL_DIR}")
    ftp = ftplib.FTP(FTP_HOST)
    ftp.login()
    ftp.cwd(FTP_PATH)
    
    files = ftp.nlst()
    # Filtra apenas zips
    files = [f for f in files if f.endswith('.zip')]
    
    # MODO DE TESTE: Baixar apenas Roraima (RR) para validar layout
    files = [f for f in files if '14_RR' in f]
    
    print(f"📋 Encontrados {len(files)} arquivos para baixar.")
    
    for filename in files:
        local_zip_path = os.path.join(LOCAL_DIR, filename)
        
        # Verifica se já foi baixado/extraído (lógica simples)
        # Se o CSV extraído já existe, pula. (O nome do CSV dentro do zip geralmente é parecido)
        # Mas como não sabemos o nome exato do CSV dentro, verificamos o zip.
        
        print(f"⬇️ Baixando {filename}...")
        start = time.time()
        with open(local_zip_path, 'wb') as f:
            ftp.retrbinary('RETR ' + filename, f.write)
        end = time.time()
        print(f"   ✅ Download concluído em {end-start:.2f}s")
        
        print(f"📦 Extraindo {filename}...")
        try:
            with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
                zip_ref.extractall(LOCAL_DIR)
            print(f"   ✅ Extração concluída.")
            
            # Opcional: Deletar o zip para economizar espaço
            os.remove(local_zip_path)
            print(f"   🗑️ Zip removido.")
            
        except zipfile.BadZipFile:
            print(f"   ❌ Erro: Arquivo zip corrompido.")

    ftp.quit()
    print("🎉 Todos os arquivos foram baixados e extraídos!")

if __name__ == "__main__":
    download_and_extract()
