import ftplib

def probe_ibge_ftp():
    ftp = ftplib.FTP('ftp.ibge.gov.br')
    ftp.login()
    
    target_path = '/Cadastro_Nacional_de_Enderecos_para_Fins_Estatisticos/Censo_Demografico_2022/Arquivos_CNEFE/CSV/UF'
    filename = '35_SP.zip'
    
    try:
        print(f"--- Checking {target_path}/{filename} ---")
        ftp.cwd(target_path)
        size = ftp.size(filename)
        print(f"Size: {size / (1024*1024):.2f} MB")
    except Exception as e:
        print(f"Error accessing {target_path}: {e}")

    ftp.quit()

if __name__ == "__main__":
    probe_ibge_ftp()
