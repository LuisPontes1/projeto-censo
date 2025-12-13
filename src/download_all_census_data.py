import requests
from bs4 import BeautifulSoup
import os
import zipfile
from urllib.parse import urljoin

BASE_URL = "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Agregados_por_Setores_Censitarios/Agregados_por_Setor_csv/"
DATA_DIR = r"c:\projetos\projeto-censo\data\raw"

def list_zip_files(url):
    print(f"Fetching file list from {url}...")
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        zip_files = []
        for link in soup.find_all('a'):
            href = link.get('href')
            if href and href.lower().endswith('.zip'):
                zip_files.append(href)
                
        return zip_files
    except Exception as e:
        print(f"Error fetching file list: {e}")
        return []

def download_file(url, dest_path):
    if os.path.exists(dest_path):
        print(f"File already exists: {dest_path}")
        return True
    
    print(f"Downloading {url} to {dest_path}...")
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(dest_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192): 
                    f.write(chunk)
        print("Download complete.")
        return True
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        return False

def unzip_file(zip_path, extract_to):
    print(f"Unzipping {zip_path}...")
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        print(f"Extracted to {extract_to}")
    except zipfile.BadZipFile:
        print(f"Error: {zip_path} is a bad zip file.")
    except Exception as e:
        print(f"Error unzipping {zip_path}: {e}")

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    
    zip_files = list_zip_files(BASE_URL)
    print(f"Found {len(zip_files)} zip files.")
    
    for filename in zip_files:
        file_url = urljoin(BASE_URL, filename)
        dest_path = os.path.join(DATA_DIR, filename)
        
        if download_file(file_url, dest_path):
            unzip_file(dest_path, DATA_DIR)

if __name__ == "__main__":
    main()
