import requests
import os
import zipfile
import pandas as pd

BASE_URL = "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Agregados_por_Setores_Censitarios/"
CSV_FOLDER_URL = BASE_URL + "Agregados_por_Setor_csv/"
DATA_DIR = r"c:\projetos\projeto-censo\data\raw"

FILES_TO_DOWNLOAD = [
    "dicionario_de_dados_agregados_por_setores_censitarios_20250417.xlsx",
]

CSV_FILES_TO_DOWNLOAD = [
    "Agregados_por_setores_basico_BR_20250417.zip"
]

def download_file(url, dest_path):
    if os.path.exists(dest_path):
        print(f"File already exists: {dest_path}")
        return
    
    print(f"Downloading {url} to {dest_path}...")
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(dest_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192): 
                    f.write(chunk)
        print("Download complete.")
    except Exception as e:
        print(f"Error downloading {url}: {e}")

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # Download Dictionary
    for f in FILES_TO_DOWNLOAD:
        url = BASE_URL + f
        dest = os.path.join(DATA_DIR, f)
        download_file(url, dest)
        
    # Download CSV Zips
    for f in CSV_FILES_TO_DOWNLOAD:
        url = CSV_FOLDER_URL + f
        dest = os.path.join(DATA_DIR, f)
        download_file(url, dest)
        
        # Unzip
        print(f"Unzipping {dest}...")
        with zipfile.ZipFile(dest, 'r') as zip_ref:
            zip_ref.extractall(DATA_DIR)
            print(f"Extracted to {DATA_DIR}")

if __name__ == "__main__":
    main()
