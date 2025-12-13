import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

BASE_URL = "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Agregados_por_Setores_Censitarios/"

def list_ibge_files(url):
    print(f"Fetching {url}...")
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        files = []
        folders = []
        
        for link in soup.find_all('a'):
            href = link.get('href')
            if href == '../': continue
            
            if href.endswith('/'):
                folders.append(href)
            else:
                files.append(href)
                
        return folders, files
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return [], []

def explore_census_structure():
    # Level 1: Root
    folders, files = list_ibge_files(BASE_URL)
    print("\n--- Root Folders ---")
    for f in folders:
        print(f)
    print("\n--- Root Files ---")
    for f in files:
        print(f)
        
    # Level 2: Check inside one folder (e.g., the first one, usually a region or BR)
    if folders:
        first_folder = folders[0]
        sub_url = BASE_URL + first_folder
        sub_folders, sub_files = list_ibge_files(sub_url)
        print(f"\n--- Inside {first_folder} ---")
        for f in sub_files:
            print(f)

if __name__ == "__main__":
    explore_census_structure()
