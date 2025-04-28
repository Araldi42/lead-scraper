import os
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import logging

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from storage_data import insert_data_to_row

load_dotenv()

def scrap_cnpj_url():
    """
    Scrape the CNPJ URL and return a list of directories.
    """
    try:
        url = os.getenv("cnpj_url")
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")
        links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.endswith("/") and href[:4].isdigit():
                links.append(href)
        return links
    except Exception as e:
        logger.error(f"Erro ao acessar a URL: {e}")
        return []


def main():
    logger.info("Iniciando raspagem de dados CNPJ...")
    print("Pastas encontradas:")
    
    for link in scrap_cnpj_url():
        base_url = os.getenv("base_url")
        pasta_url = f"{base_url}{link}"
        print(f"Processando pasta: {pasta_url}")
        
        # Listar arquivos dentro da pasta
        response = requests.get(pasta_url)
        soup = BeautifulSoup(response.text, "html.parser")
        arquivos = []
        
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not href.endswith("/") and (href.endswith(".zip") or href.endswith(".txt") or href.endswith(".csv")):
                arquivos.append(href)
        
        print(f"Arquivos encontrados: {len(arquivos)}")
        
        # Baixar e processar cada arquivo
        for arquivo in arquivos:
            try:
                file_url = f"{pasta_url}{arquivo}"
                print(f"Baixando arquivo: {file_url}")
                
                # Fazer o download do arquivo
                download_response = requests.get(file_url)
                if download_response.status_code == 200:
                    # Salvar no MinIO através da função insert_data_to_row
                    insert_data_to_row(arquivo, download_response.content, arquivo.split(".")[-1])
                else:
                    logger.error(f"Falha ao baixar {file_url}. Status code: {download_response.status_code}")
            except Exception as e:
                logger.error(f"Erro ao processar arquivo {arquivo}: {e}")
            
            # Remova este break para processar todos os arquivos
        
        # Remova este break para processar todas as pastas
        break
    
    logger.info("Raspagem concluída!")

if __name__ == "__main__":
    main()

