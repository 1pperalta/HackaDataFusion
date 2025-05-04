#!/usr/bin/env python
"""
GitHub Analytics S3 Uploader para datos procesados

Este script sube los datos procesados (en la carpeta processed/gold) a un bucket de S3 en AWS,
manteniendo la misma estructura de directorios.

Características:
- Sincronización de archivos procesados con S3
- Mantiene la estructura de carpetas en S3 (processed/gold/...)
- Verificación de archivos ya existentes para evitar subidas duplicadas
- Comprobación de integridad mediante hashes MD5
- Soporte para carga paralela de archivos
- Gestión segura de credenciales desde .env
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

# Configurar logging antes de todo
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/s3_processed_uploader.log", mode='a', encoding='utf-8')
    ]
)
logger = logging.getLogger("s3_processed_uploader")

# Asegurar que el directorio de logs existe
Path("logs").mkdir(parents=True, exist_ok=True)

# Detectar la raíz del proyecto
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR
# Navegar hacia arriba hasta encontrar la raíz del proyecto (donde está la carpeta data o hasta llegar a 4 niveles)
for _ in range(4):
    if (PROJECT_ROOT / "data").exists():
        break
    PROJECT_ROOT = PROJECT_ROOT.parent

# Importar dotenv aquí, después de detectar la raíz del proyecto
try:
    from dotenv import load_dotenv
    
    # Intentar cargar .env desde varias ubicaciones
    env_loaded = False
    env_paths = [
        PROJECT_ROOT / '.env',
        Path('.env'),
        Path('../../.env'),
        Path('../../../.env'),
        Path('/Users/andresariasmedina/Documents/HackaDataFusion/.env')
    ]
    
    for env_path in env_paths:
        if env_path.exists():
            print(f"📝 Cargando variables de entorno desde: {env_path}")
            load_dotenv(dotenv_path=env_path, override=True)
            env_loaded = True
            break
    
    if not env_loaded:
        print("⚠️ No se encontró archivo .env")
        
    # Verificar si se cargaron las credenciales
    if os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"):
        print("✅ Credenciales de AWS cargadas correctamente desde .env")
    else:
        print("⚠️ No se encontraron credenciales AWS en las variables de entorno")
        
except ImportError:
    print("⚠️ Error: No se pudo importar dotenv. Instalándolo...")
    try:
        import pip
        pip.main(['install', 'python-dotenv'])
        from dotenv import load_dotenv
        
        # Intentar cargar .env después de instalar
        if (PROJECT_ROOT / '.env').exists():
            load_dotenv(PROJECT_ROOT / '.env')
            print("✅ dotenv instalado y .env cargado correctamente")
        else:
            print("⚠️ No se encontró archivo .env después de instalar dotenv")
    except Exception as e:
        print(f"❌ Error al instalar dotenv: {e}")
        print("   Por favor, instale python-dotenv manualmente: pip install python-dotenv")

# Directorio de datos procesados
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed" / "gold"

print(f"📂 Script ubicado en: {SCRIPT_DIR}")
print(f"📂 Raíz del proyecto detectada en: {PROJECT_ROOT}")
print(f"📂 Buscando datos procesados en: {PROCESSED_DATA_DIR}")


class S3ProcessedUploader:
    """Gestor de subida de datos procesados a S3"""
    
    def __init__(
        self,
        bucket_name: str,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_profile: Optional[str] = None,
        region_name: str = 'us-east-1',
        max_workers: int = 5,
        local_dir: Path = PROCESSED_DATA_DIR,
        s3_prefix: str = 'processed/gold'
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
        
        # Verificar que el directorio existe
        if not self.local_dir.exists():
            logger.warning(f"El directorio {self.local_dir} no existe. Buscando alternativas...")
            # Buscar en ubicaciones alternativas
            alternative_paths = [
                Path.cwd() / "data" / "processed" / "gold",
                PROJECT_ROOT / "data" / "processed" / "gold",
                Path("/Users/andresariasmedina/Documents/HackaDataFusion/data/processed/gold")
            ]
            
            for path in alternative_paths:
                if path.exists():
                    self.local_dir = path
                    logger.info(f"Usando directorio alternativo: {self.local_dir}")
                    break
            
            if not self.local_dir.exists():
                raise FileNotFoundError(f"No se encontró el directorio de datos procesados en ninguna ubicación.")
        
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
        relative_path = local_path.relative_to(self.local_dir.parent.parent)
        
        # Combinar con el prefijo de S3 (sin duplicar la estructura)
        # Esto asegura que los archivos en processed/gold se suban a processed/gold en S3
        s3_key = str(relative_path).replace('\\', '/')
        
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
        Encuentra todos los archivos procesados en el directorio local
        
        Returns:
            Lista de rutas a archivos
        """
        files = []
        
        # Buscar recursivamente todos los archivos (no solo .json.gz)
        for item in self.local_dir.glob('**/*'):
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
        logger.info(f"Encontrados {len(local_files)} archivos para subir en {self.local_dir}")
        
        if not local_files:
            logger.warning("No se encontraron archivos para subir")
            return {}
        
        # Mostrar los primeros 5 archivos para verificar
        for i, file in enumerate(local_files[:5]):
            rel_path = file.relative_to(self.local_dir.parent.parent)
            s3_key = str(rel_path).replace('\\', '/')
            print(f"  Archivo {i+1}: {file.name} -> s3://{self.bucket_name}/{s3_key}")
        
        if len(local_files) > 5:
            print(f"  ... y {len(local_files) - 5} archivos más")
        
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
                Prefix='processed/gold'  # Buscar solo en el prefijo de datos procesados
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
                'prefix': 'processed/gold'
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
    parser = argparse.ArgumentParser(description="Subir datos procesados a S3")
    
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
        default="processed/gold",
        help="Prefijo (carpeta) para los archivos en S3 (default: processed/gold)"
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
    
    # Directorio de datos procesados
    parser.add_argument(
        "--data-dir",
        type=str,
        help="Directorio con datos procesados (default: data/processed/gold en la raíz del proyecto)"
    )
    
    # Credenciales directas
    parser.add_argument(
        "--access-key",
        type=str,
        help="AWS Access Key ID (en lugar de usar .env)"
    )
    
    parser.add_argument(
        "--secret-key",
        type=str,
        help="AWS Secret Access Key (en lugar de usar .env)"
    )
    
    # Archivo .env específico
    parser.add_argument(
        "--env-file",
        type=str,
        help="Ruta al archivo .env (en lugar de usar la ubicación por defecto)"
    )
    
    return parser.parse_args()


def main():
    """Función principal"""
    try:
        # Parsear argumentos
        args = parse_arguments()
        
        # Si se especificó un archivo .env, cargarlo
        if args.env_file:
            if Path(args.env_file).exists():
                try:
                    from dotenv import load_dotenv
                    load_dotenv(args.env_file, override=True)
                    print(f"✅ Variables de entorno cargadas desde {args.env_file}")
                except ImportError:
                    print("⚠️ No se pudo importar dotenv para cargar el archivo .env especificado")
            else:
                print(f"⚠️ El archivo .env especificado no existe: {args.env_file}")
        
        # Obtener credenciales de las variables de entorno si no se usa un perfil o credenciales directas
        aws_access_key_id = args.access_key or os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = args.secret_key or os.getenv("AWS_SECRET_ACCESS_KEY")
        
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
            print("   3. Pase las credenciales directamente con --access-key y --secret-key")
            print("   4. Especifique la ruta al archivo .env con --env-file")
            print("\n   Variables de entorno actuales:")
            print(f"   - AWS_ACCESS_KEY_ID: {'✅ Configurada' if os.getenv('AWS_ACCESS_KEY_ID') else '❌ No configurada'}")
            print(f"   - AWS_SECRET_ACCESS_KEY: {'✅ Configurada' if os.getenv('AWS_SECRET_ACCESS_KEY') else '❌ No configurada'}")
            print(f"   - AWS_S3_BUCKET: {os.getenv('AWS_S3_BUCKET') or '❌ No configurada'}")
            return 1
        
        # Verificar que se haya especificado un bucket
        if not args.bucket:
            logger.error("Se debe especificar un bucket de S3")
            print("\n❌ Error: Se debe especificar un bucket de S3 con --bucket o la variable de entorno AWS_S3_BUCKET")
            return 1
        
        # Directorio de datos procesados
        data_dir = PROCESSED_DATA_DIR
        if args.data_dir:
            data_dir = Path(args.data_dir)
        
        # Inicializar y ejecutar el uploader
        uploader = S3ProcessedUploader(
            bucket_name=args.bucket,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_profile=args.profile,
            region_name=args.region,
            max_workers=args.max_workers,
            local_dir=data_dir,
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