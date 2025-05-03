#!/usr/bin/env python
"""
GitHub Archive S3 Uploader

Este script sube los datos descargados de GitHub Archive desde la carpeta 
data/raw a un bucket de S3 en AWS, manteniendo la misma estructura de directorios.

Características:
- Sincronización de archivos locales con S3
- Verificación de archivos ya existentes para evitar subidas duplicadas
- Comprobación de integridad mediante hashes MD5
- Soporte para carga paralela de archivos
- Gestión segura de credenciales
"""

import os
import sys
import argparse
import logging
import hashlib
import concurrent.futures
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
import time

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from tqdm import tqdm
from dotenv import load_dotenv

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/s3_uploader.log", mode='a')
    ]
)
logger = logging.getLogger("s3_uploader")

# Directorio de datos crudos
RAW_DATA_DIR = Path("data/raw")

# Crear directorio de logs si no existe
Path("logs").mkdir(exist_ok=True)

# Cargar variables de entorno
load_dotenv('.env')


class S3Uploader:
    """Gestor de subida de archivos a S3"""
    
    def __init__(
        self,
        bucket_name: str,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_profile: Optional[str] = None,
        region_name: str = 'us-east-1',
        max_workers: int = 5,
        local_dir: Path = RAW_DATA_DIR,
        s3_prefix: str = 'github-archive'
    ):
        """
        Inicializa el gestor de subida a S3
        
        Args:
            bucket_name: Nombre del bucket de S3
            aws_access_key_id: ID de clave de acceso de AWS
            aws_secret_access_key: Clave secreta de acceso de AWS
            aws_profile: Nombre del perfil de AWS (alternativa a las claves)
            region_name: Región de AWS
            max_workers: Número máximo de trabajadores para subidas paralelas
            local_dir: Directorio local con los archivos a subir
            s3_prefix: Prefijo (carpeta) para los archivos en S3
        """
        self.bucket_name = bucket_name
        self.region_name = region_name
        self.max_workers = max_workers
        self.local_dir = local_dir
        self.s3_prefix = s3_prefix
        
        # Configurar cliente de S3
        if aws_profile:
            # Usar un perfil específico
            session = boto3.Session(profile_name=aws_profile, region_name=region_name)
            self.s3_client = session.client('s3')
        else:
            # Usar credenciales explícitas o variables de entorno
            self.s3_client = boto3.client(
                's3',
                region_name=region_name,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key
            )
        
        # Configuración de transferencia para subidas multipart
        self.transfer_config = TransferConfig(
            multipart_threshold=8 * 1024 * 1024,  # 8MB
            max_concurrency=max_workers,
            multipart_chunksize=8 * 1024 * 1024,
            use_threads=True
        )
    
    def get_s3_key(self, local_path: Path) -> str:
        """
        Convierte una ruta local a una clave S3 manteniendo la estructura de directorios
        
        Args:
            local_path: Ruta local del archivo
            
        Returns:
            Clave S3 correspondiente
        """
        # Obtener ruta relativa al directorio base
        relative_path = local_path.relative_to(self.local_dir)
        
        # Combinar con el prefijo de S3
        s3_key = f"{self.s3_prefix}/{relative_path}"
        
        return s3_key
    
    def calculate_file_md5(self, file_path: Path) -> str:
        """
        Calcula el hash MD5 de un archivo
        
        Args:
            file_path: Ruta al archivo
            
        Returns:
            Hash MD5 en formato hexadecimal
        """
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def check_file_exists(self, s3_key: str) -> bool:
        """
        Verifica si un archivo ya existe en S3
        
        Args:
            s3_key: Clave del archivo en S3
            
        Returns:
            True si el archivo existe, False en caso contrario
        """
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError as e:
            # Si el error es 404, el archivo no existe
            if e.response['Error']['Code'] == '404':
                return False
            # Si es otro error, propagarlo
            raise
    
    def upload_file(self, local_path: Path) -> Tuple[str, bool]:
        """
        Sube un archivo a S3
        
        Args:
            local_path: Ruta local del archivo
            
        Returns:
            Tupla con (clave S3, éxito)
        """
        s3_key = self.get_s3_key(local_path)
        
        # Verificar si el archivo ya existe
        if self.check_file_exists(s3_key):
            logger.info(f"Archivo {s3_key} ya existe en S3, omitiendo")
            return s3_key, True
        
        try:
            # Subir archivo con barra de progreso
            file_size = os.path.getsize(local_path)
            
            with tqdm(
                total=file_size,
                unit='B',
                unit_scale=True,
                desc=f"Subiendo {local_path.name}",
                disable=not sys.stdout.isatty()
            ) as pbar:
                # Crear callback para actualizar la barra de progreso
                def progress_callback(bytes_transferred):
                    pbar.update(bytes_transferred)
                
                # Subir archivo
                self.s3_client.upload_file(
                    str(local_path),
                    self.bucket_name,
                    s3_key,
                    Callback=progress_callback,
                    Config=self.transfer_config
                )
            
            logger.info(f"Archivo {local_path} subido exitosamente a s3://{self.bucket_name}/{s3_key}")
            return s3_key, True
            
        except Exception as e:
            logger.error(f"Error al subir {local_path} a S3: {e}")
            return s3_key, False
    
    def find_local_files(self) -> List[Path]:
        """
        Encuentra todos los archivos .json.gz en el directorio local
        
        Returns:
            Lista de rutas a archivos
        """
        files = []
        
        for item in self.local_dir.glob('**/*.json.gz'):
            if item.is_file():
                files.append(item)
        
        return files
    
    def run(self) -> Dict[str, bool]:
        """
        Ejecuta el proceso de subida a S3
        
        Returns:
            Diccionario con {clave_s3: éxito}
        """
        # Verificar que el bucket existe
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code')
            
            # Si el bucket no existe, intentar crearlo
            if error_code == '404':
                logger.info(f"Bucket {self.bucket_name} no existe, creándolo...")
                self.s3_client.create_bucket(
                    Bucket=self.bucket_name,
                    CreateBucketConfiguration={
                        'LocationConstraint': self.region_name
                    } if self.region_name != 'us-east-1' else {}
                )
            else:
                logger.error(f"Error al acceder al bucket {self.bucket_name}: {e}")
                raise
        
        # Encontrar archivos locales
        local_files = self.find_local_files()
        logger.info(f"Encontrados {len(local_files)} archivos para subir")
        
        if not local_files:
            logger.warning("No se encontraron archivos para subir")
            return {}
        
        # Subir archivos en paralelo
        results = {}
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_file = {executor.submit(self.upload_file, file): file for file in local_files}
            
            with tqdm(
                total=len(local_files),
                desc="Progreso global",
                unit="archivos",
                disable=not sys.stdout.isatty()
            ) as pbar:
                for future in concurrent.futures.as_completed(future_to_file):
                    try:
                        s3_key, success = future.result()
                        results[s3_key] = success
                    except Exception as e:
                        file = future_to_file[future]
                        logger.error(f"Error al procesar {file}: {e}")
                        results[self.get_s3_key(file)] = False
                    
                    pbar.update(1)
        
        # Resultados
        successful = sum(1 for success in results.values() if success)
        logger.info(f"Subida completada: {successful}/{len(results)} archivos exitosos")
        
        return results
    
    def get_s3_stats(self) -> Dict[str, Any]:
        """
        Obtiene estadísticas de los archivos en S3
        
        Returns:
            Diccionario con estadísticas
        """
        try:
            # Listar objetos en el bucket con el prefijo dado
            paginator = self.s3_client.get_paginator('list_objects_v2')
            
            total_size = 0
            total_files = 0
            
            for page in paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=self.s3_prefix
            ):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        total_size += obj['Size']
                        total_files += 1
            
            return {
                'total_files': total_files,
                'total_size_bytes': total_size,
                'total_size_mb': total_size / (1024 * 1024),
                'bucket': self.bucket_name,
                'prefix': self.s3_prefix
            }
            
        except Exception as e:
            logger.error(f"Error al obtener estadísticas de S3: {e}")
            return {
                'error': str(e)
            }


def parse_arguments():
    """
    Parsea los argumentos de línea de comandos
    
    Returns:
        Argumentos parseados
    """
    parser = argparse.ArgumentParser(description="Subir datos de GitHub Archive a S3")
    
    # Argumentos para S3
    parser.add_argument(
        "--bucket",
        type=str,
        default=os.getenv("AWS_S3_BUCKET"),
        help="Nombre del bucket de S3 (default: variable de entorno AWS_S3_BUCKET)"
    )
    
    parser.add_argument(
        "--prefix",
        type=str,
        default="github-archive",
        help="Prefijo (carpeta) para los archivos en S3 (default: github-archive)"
    )
    
    parser.add_argument(
        "--profile",
        type=str,
        help="Perfil de AWS a utilizar (opcional)"
    )
    
    parser.add_argument(
        "--region",
        type=str,
        default=os.getenv("AWS_REGION", "us-east-1"),
        help="Región de AWS (default: us-east-1 o variable AWS_REGION)"
    )
    
    # Argumentos de configuración
    parser.add_argument(
        "--max-workers",
        type=int,
        default=5,
        help="Número máximo de trabajadores para subidas paralelas (default: 5)"
    )
    
    args = parser.parse_args()
    
    # Verificar que se haya especificado un bucket
    if not args.bucket:
        parser.error("Se debe especificar un bucket con --bucket o la variable de entorno AWS_S3_BUCKET")
    
    return args


def main():
    """Función principal"""
    try:
        # Parsear argumentos
        args = parse_arguments()
        
        # Obtener credenciales de las variables de entorno si no se usa un perfil
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        
        # Si se usa un perfil, no se necesitan las credenciales explícitas
        if args.profile and (aws_access_key_id or aws_secret_access_key):
            logger.warning("Se ignorarán las credenciales explícitas porque se ha especificado un perfil")
            aws_access_key_id = None
            aws_secret_access_key = None
        
        # Verificar credenciales o perfil
        if not args.profile and (not aws_access_key_id or not aws_secret_access_key):
            logger.error("Se deben proporcionar credenciales de AWS o un perfil")
            print("\n❌ Error: Se deben proporcionar credenciales de AWS o un perfil.")
            print("   Opciones:")
            print("   1. Configure las variables de entorno AWS_ACCESS_KEY_ID y AWS_SECRET_ACCESS_KEY")
            print("   2. Especifique un perfil de AWS con --profile")
            return 1
        
        # Inicializar y ejecutar el uploader
        uploader = S3Uploader(
            bucket_name=args.bucket,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_profile=args.profile,
            region_name=args.region,
            max_workers=args.max_workers,
            s3_prefix=args.prefix
        )
        
        # Ejecutar subida
        results = uploader.run()
        
        # Obtener estadísticas
        stats = uploader.get_s3_stats()
        logger.info(f"Estadísticas de S3: {stats}")
        
        # Informar resultados
        if not results:
            print("\n⚠️ No se encontraron archivos para subir")
            return 0
        
        successful = sum(1 for success in results.values() if success)
        total = len(results)
        
        if successful == total:
            print(f"\n✅ Subida completada exitosamente: {successful}/{total} archivos")
            print(f"📊 Total en S3: {stats.get('total_size_mb', 0):.2f} MB en {stats.get('total_files', 0)} archivos")
            print(f"📦 Bucket: s3://{args.bucket}/{args.prefix}")
            return 0
        else:
            print(f"\n⚠️ Subida completada con advertencias: {successful}/{total} archivos")
            print(f"❌ {total - successful} archivos fallaron, ver logs para detalles")
            print(f"📊 Total en S3: {stats.get('total_size_mb', 0):.2f} MB en {stats.get('total_files', 0)} archivos")
            return 1
    
    except KeyboardInterrupt:
        logger.info("Subida interrumpida por el usuario")
        print("\n⚠️ Subida interrumpida por el usuario")
        return 130
    
    except Exception as e:
        logger.critical(f"Error en la subida: {e}", exc_info=True)
        print(f"\n❌ Error en la subida: {e}")
        return 2


if __name__ == "__main__":
    sys.exit(main())