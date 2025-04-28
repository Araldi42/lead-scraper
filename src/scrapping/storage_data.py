import os
import sys
from dotenv import load_dotenv
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.minioHandler import MinioHandler

load_dotenv()

minio = MinioHandler(
    host=os.getenv("minio_host"),
    port=os.getenv("minio_port"),
    access_key=os.getenv("minio_access_key"),
    secret_key=os.getenv("minio_secret_key"),
)

def insert_data_to_row(file_name, file_content, file_type):
    try:
        # Criar bucket 'raw' - se já existir, esse método já trata internamente
        bucket_name = "raw"
        minio.create_bucket(bucket_name)
        minio.set_bucket(bucket_name)
        
        # Determinar a pasta de destino
        target_folder = ""
        if file_name.startswith("Empresas"):
            target_folder = "empresas"
        elif file_name.startswith("Estabelecimentos"):
            target_folder = "estabelecimentos"
        elif file_name.startswith("Socios"):
            target_folder = "socios"
        else:
            target_folder = file_name.split(".")[0].lower()
        
        # Criar a pasta de destino
        minio.create_subfolder(target_folder)
        
        # Upload do conteúdo diretamente para o MinIO
        object_name = f"{target_folder}/{file_name.split('.')[0]}_{datetime.now().strftime('%Y-%m')}.{file_type}"
        minio.upload_content(object_name,
                             file_content,
                             file_type)
        print(f"Arquivo {file_name} salvo em {target_folder}")
                
    except Exception as e:
        print(f"Erro ao salvar arquivo no MinIO: {e}")




