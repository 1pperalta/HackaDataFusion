import os
import logging
import subprocess
import boto3
from datetime import datetime
from botocore.exceptions import NoCredentialsError

# Configurar el logger del pipeline principal
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
log_dir = os.path.join(project_root, 'logs')
os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(log_dir, 'pipeline.log')

logging.basicConfig(
    filename=log_file,
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s - pipeline - %(levelname)s - %(message)s'
)

def log_info(msg):
    print(msg)
    logging.info(msg)

def log_error(msg):
    print(msg)
    logging.error(msg)

def ejecutar_comando(comando):
    try:
        result = subprocess.run(
            comando,
            shell=True,
            capture_output=True,
            text=True
        )
        if result.stdout:
            for line in result.stdout.strip().split("\n"):
                log_info(f"[OUT] {line}")
        if result.stderr:
            for line in result.stderr.strip().split("\n"):
                log_error(f"[ERR] {line}")
        return result.returncode == 0
    except Exception as e:
        log_error(f"Excepción al ejecutar comando: {e}")
        return False

def subir_a_s3(archivos):
    """
    Subir los archivos procesados a un bucket S3.
    """
    log_info("🔼 Subiendo archivos a S3...")
    
    # Configuración de AWS S3
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION")
    )
    
    bucket_name = os.getenv("S3_BUCKET_NAME")
    
    for archivo in archivos:
        try:
            archivo_name = os.path.basename(archivo)
            log_info(f"Subiendo archivo {archivo_name} a S3...")
            s3_client.upload_file(archivo, bucket_name, f'processed/gold/{archivo_name}')
            log_info(f"✅ {archivo_name} subido exitosamente a S3.")
        except FileNotFoundError:
            log_error(f"⚠️ El archivo {archivo} no se encontró.")
        except NoCredentialsError:
            log_error("⚠️ No se encontraron credenciales válidas para AWS.")
        except Exception as e:
            log_error(f"⚠️ Error al subir {archivo} a S3: {e}")

def main():
    log_info(f"Raíz del proyecto: {project_root}")
    log_info("🟡 Iniciando ejecución del pipeline completo (Medallion)")

    # CAPA BRONZE
    log_info("🟤 Ejecutando capa Bronze...")
    cmd_bronze = "python src/data_flow/data_preprocessing/bronze_processor.py"
    if not ejecutar_comando(cmd_bronze):
        log_error("Error en Bronze. Abortando.")
        return

    # CAPA SILVER
    log_info("⚪ Ejecutando capa Silver...")
    cmd_silver = "python src/data_flow/data_preprocessing/silver_processor.py"
    if not ejecutar_comando(cmd_silver):
        log_error("Error en Silver. Abortando.")
        return

    # CAPA GOLD
    log_info("🟢 Ejecutando capa Gold...")
    cmd_gold = "python src/data_flow/data_preprocessing/gold_processor.py"
    if not ejecutar_comando(cmd_gold):
        log_error("Error en Gold. Abortando.")
        return

    # Subir a S3 después de procesar la capa Gold
    processed_gold_dir = os.path.join(project_root, 'data', 'processed', 'gold')
    archivos_gold = [os.path.join(processed_gold_dir, file) for file in os.listdir(processed_gold_dir) if file.endswith('.json.gz')]
    
    if archivos_gold:
        subir_a_s3(archivos_gold)

    log_info("🟢 Pipeline finalizado correctamente.")

if __name__ == "__main__":
    main()
