import os
import logging
from minio import Minio
from minio.error import S3Error, MinioException
from minio.commonconfig import CopySource
from io import BytesIO
logging.basicConfig(level=logging.INFO)
logging.getLogger("urllib3").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

class MinioHandler():
    ''' Classe para manipulação de buckets no MinIO
    ---------------------------
    Parâmetros:
    host (str): Endereço do servidor MinIO
    port (int): Porta do servidor MinIO
    access_key (str): Chave de acesso
    secret_key (str): Chave secreta
    ---------------------------
    '''
    def __init__(self, host: str, port: int, access_key: str, secret_key: str) -> None :
        self.__host = host
        self.__port = port
        self.__access_key = access_key
        self.__secret_key = secret_key
        self.__client = None
        self.__bucket_name = None
        self.__connect()

    def __connect(self) -> None:
        try:
            self.__client = Minio(
                f"{self.__host}:{self.__port}",
                access_key=self.__access_key,
                secret_key=self.__secret_key,
                secure=False
            )
        except MinioException as err:
            logger.error("Erro ao conectar ao MinIO: %s", err)


    def set_bucket(self, bucket_name: str) -> None:
        ''' Define o bucket a ser manipulado
        ---------------------------
        Parâmetros:
        bucket_name (str): Nome do bucket a ser manipulado
        ---------------------------
        Retorno:
        None
        ---------------------------
        '''
        self.__bucket_name = bucket_name

    def create_bucket(self, bucket_name: str) -> None:
        ''' Cria um bucket no MinIO
        ---------------------------
        Parâmetros:
        bucket_name (str): Nome do bucket a ser criado
        ---------------------------
        Retorno:
        None
        ---------------------------
        '''
        try:
            if not self.__client.bucket_exists(bucket_name):
                self.__client.make_bucket(bucket_name)
                logger.info("Bucket %s criado com sucesso.", bucket_name)
            else:
                logger.info("Bucket %s já existe.", bucket_name)
                pass
        except S3Error as err:
            logger.error("Erro ao verificar ou criar o bucket: %s", err)

    def list_buckets(self) -> None:
        ''' Lista todos os buckets existentes no MinIO
        ---------------------------
        Parâmetros:
        None
        ---------------------------
        Retorno:
        None
        ---------------------------
        '''
        try:
            buckets = self.__client.list_buckets()
            logger.info("Buckets existentes:")
            for bucket in buckets:
                print(f" - {bucket.name}")
        except S3Error as err:
            logger.error("Erro ao listar buckets: %s", err)

    def upload_file(self, file_path: str, object_name: str = None) -> None:
        ''' Envia um arquivo para o bucket
        ---------------------------
        Parâmetros:
        file_path (str): Caminho do arquivo a ser enviado
        object_name (str, opcional): Nome do objeto no bucket. Se não for fornecido, 
                                     usa o nome do arquivo
        ---------------------------
        Retorno:
        None
        ---------------------------
        '''
        try:
            if object_name is None:
                object_name = os.path.basename(file_path)
            
            self.__client.fput_object(self.__bucket_name, object_name, file_path)
            logger.info("Arquivo %s enviado com sucesso para %s.", file_path, object_name)
        except S3Error as err:
            logger.error("Erro ao enviar arquivo: %s", err)

    def download_file(self, file_path: str) -> None:
        ''' Baixa um arquivo do bucket
        ---------------------------
        Parâmetros:
        file_path (str): Caminho do arquivo a ser baixado
        ---------------------------
        Retorno:
        None
        ---------------------------
        '''
        try:
            object_name = os.path.basename(file_path)
            self.__client.fget_object(self.__bucket_name, object_name, file_path)
            print("Arquivo %s baixado com sucesso.", file_path)
        except S3Error as err:
            logger.error("Erro ao baixar arquivo: %s", err)

    def list_files(self) -> None:
        ''' Lista todos os arquivos existentes no bucket
        ---------------------------
        Parâmetros:
        None
        ---------------------------
        Retorno:
        None
        ---------------------------
        '''
        try:
            objects = self.__client.list_objects(self.__bucket_name, recursive=True)
            logger.info("Arquivos no bucket %s:", self.__bucket_name)
            for obj in objects:
                print(f" - {obj.object_name}")
        except S3Error as err:
            logger.error("Erro ao listar arquivos: %s", err)

    def get_files(self) -> dict:
        ''' Retorna o conteúdo de um arquivo do bucket
        ---------------------------
        Parâmetros:
        None
        ---------------------------
        Retorno:
        dict: Dicionário com o nome do arquivo e seu conteúdo
            Key (str): Nome do arquivo
            Value (bytes): Conteúdo do arquivo
        ---------------------------
        '''
        try:
            data_dict = {}
            for object_name in [obj.object_name for obj in self.__client.list_objects(self.__bucket_name)]:
                data = self.__client.get_object(self.__bucket_name, object_name)
                data_dict[object_name] = data.read()
            return data_dict
        except S3Error as err:
            logger.error("Erro ao obter arquivo: %s", err)
            return {}

    def delete_file(self, file_path: str) -> None:
        ''' Remove um arquivo do bucket
        ---------------------------
        Parâmetros:
        file_path (str): Caminho do arquivo a ser removido
        ---------------------------
        Retorno:
        None
        ---------------------------
        '''
        try:
            object_name = os.path.basename(file_path)
            self.__client.remove_object(self.__bucket_name, object_name)
            logger.info("Arquivo %s removido com sucesso.", file_path)
        except S3Error as err:
            logger.error("Erro ao remover arquivo: %s", err)

    def delete_bucket(self) -> None:
        ''' Remove o bucket
        ---------------------------
        Parâmetros:
        None
        ---------------------------
        Retorno:
        None
        ---------------------------
        '''
        try:
            self.__client.remove_bucket(self.__bucket_name)
            logger.info("Bucket %s removido com sucesso.", self.__bucket_name)
        except S3Error as err:
            logger.error("Erro ao remover bucket: %s", err)

    def move_file(self, object_name: str, new_bucket: str) -> None:
        ''' Move um arquivo de um bucket para outro
        ---------------------------
        Parâmetros:
        file_path (str): Caminho do arquivo a ser movido
        new_bucket (str): Nome do novo bucket
        ---------------------------
        Retorno:
        None
        ---------------------------
        '''
        try:
            source = CopySource(self.__bucket_name, object_name)
            self.__client.copy_object(new_bucket, object_name, source)
            self.__client.remove_object(self.__bucket_name, object_name)
            logger.info("Arquivo %s movido com sucesso.", object_name)
        except S3Error as err:
            logger.error("Erro ao mover arquivo: %s", err)

    def create_subfolder(self, subfolder_path: str) -> None:
        '''
        Cria uma subpasta (objeto vazio com "/" no final do nome) dentro do bucket atual.
        ---------------------------
        Parâmetros:
        subfolder_path (str): Caminho da subpasta (ex: "pasta1/subpasta2/")
        ---------------------------
        Retorno:
        None
        ---------------------------
        '''
        try:
            if not subfolder_path.endswith('/'):
                subfolder_path += '/'
            
            objetos = self.__client.list_objects(self.__bucket_name, prefix=subfolder_path, recursive=False)
            objetos_list = list(objetos)
            
            if not any(obj.object_name == subfolder_path for obj in objetos_list):
                empty_stream = BytesIO(b'')
                self.__client.put_object(
                    self.__bucket_name,
                    subfolder_path,
                    data=empty_stream,
                    length=0
                )
                logger.info("Subpasta '%s' criada com sucesso.", subfolder_path)
            else:
                logger.info("Subpasta '%s' já existe.", subfolder_path)
        except S3Error as err:
            logger.error("Erro ao criar subpasta: %s", err)

    def list_folder(self, prefix: str) -> list:
        '''
        Lista arquivos e subpastas diretamente dentro de um prefixo (pasta) no bucket atual.
        ---------------------------
        Parâmetros:
        prefix (str): Prefixo da pasta (ex: "pasta1/")
        ---------------------------
        Retorno:
        list: Lista de nomes de arquivos e subpastas diretamente dentro do prefixo
        ---------------------------
        '''
        try:
            objetos = self.__client.list_objects(self.__bucket_name, prefix=prefix, recursive=False)
            resultado = [obj.object_name for obj in objetos]
            logger.info("Conteúdo da pasta '%s':", prefix)
            for nome in resultado:
                print(f" - {nome}")
            return resultado
        except S3Error as err:
            logger.error("Erro ao listar conteúdo da pasta: %s", err)
            return []

    def upload_content(self, object_name: str, content: bytes, type: str) -> None:
        ''' Envia conteúdo em bytes para o bucket
        ---------------------------
        Parâmetros:
        object_name (str): Nome do objeto no bucket
        content (bytes): Conteúdo em bytes para upload
        type (str): Tipo de conteúdo (ex: "csv", "txt", "zip")
        ---------------------------
        Retorno:
        None
        ---------------------------
        '''
        try:
            content_bytes = BytesIO(content)
            content_bytes.seek(0)
            size = len(content)
            try:
                # Upload do arquivo para o bucket
                self.__client.put_object(
                    self.__bucket_name,
                    object_name,
                    data=content_bytes,
                    length=size,
                    content_type=f"application/{type}"
                )
                logger.info("Conteúdo enviado com sucesso para %s.", object_name)    
            except S3Error as err:
                logger.error("Erro ao enviar conteúdo: %s", err)
        except Exception as err:
            logger.error("Erro ao converter conteúdo: %s", err)
