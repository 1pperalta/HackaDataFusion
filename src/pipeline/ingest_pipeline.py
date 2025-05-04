#!/usr/bin/env python
"""
Pipeline de ingestión para HackaDataFusion

Este script ejecuta secuencialmente los componentes de ingestión:
1. Descarga de datos de GitHub Archive
2. Subida de datos a S3 (verificando credenciales AWS)

Características:
- Tolerante a fallos parciales en la descarga
- Validación previa de credenciales AWS
- Manejo de variables de entorno .env
- Configurable mediante argumentos
"""

import os
import sys
import argparse
import subprocess
import datetime
import re
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

def parse_arguments():
    """Parsea los argumentos de línea de comandos"""
    parser = argparse.ArgumentParser(description="Pipeline de ingestión para GitHub Archive")
    
    # Fechas para la descarga
    parser.add_argument(
        "--start-date",
        type=str,
        default=(datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
        help="Fecha de inicio en formato YYYY-MM-DD (default: ayer)"
    )
    
    parser.add_argument(
        "--end-date",
        type=str,
        default=datetime.datetime.now().strftime("%Y-%m-%d"),
        help="Fecha de fin en formato YYYY-MM-DD (default: hoy)"
    )
    
    # Configuración de S3
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
        help="Prefijo para los archivos en S3 (default: github-archive)"
    )
    
    parser.add_argument(
        "--region",
        type=str,
        default=os.getenv("AWS_REGION", "us-east-1"),
        help="Región de AWS (default: us-east-1 o variable AWS_REGION)"
    )
    
    # Opción de perfil
    parser.add_argument(
        "--profile",
        type=str,
        default=os.getenv("AWS_PROFILE"),
        help="Perfil de AWS a utilizar (default: variable de entorno AWS_PROFILE)"
    )
    
    # Opciones adicionales
    parser.add_argument(
        "--force-continue",
        action="store_true",
        help="Continuar con la subida a S3 incluso si la descarga falla completamente"
    )
    
    parser.add_argument(
        "--skip-s3",
        action="store_true",
        help="Omitir la subida a S3 y ejecutar solo la descarga"
    )
    
    return parser.parse_args()

def validate_aws_credentials():
    """
    Valida que existan credenciales de AWS disponibles
    
    Returns:
        Tupla con (existe_credenciales, método_autenticación)
    """
    # Verificar si hay credenciales en variables de entorno
    if os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"):
        return True, "variables de entorno AWS_ACCESS_KEY_ID y AWS_SECRET_ACCESS_KEY"
    
    # Verificar si hay un perfil de AWS configurado
    if os.getenv("AWS_PROFILE"):
        return True, f"perfil AWS_PROFILE={os.getenv('AWS_PROFILE')}"
    
    # Verificar si hay credenciales en ~/.aws/credentials
    aws_credentials_path = Path.home() / ".aws" / "credentials"
    if aws_credentials_path.exists():
        return True, f"archivo de credenciales {aws_credentials_path}"
    
    # Verificar si estamos en una instancia EC2 con rol IAM
    if os.path.exists('/sys/hypervisor/uuid'):
        with open('/sys/hypervisor/uuid', 'r') as f:
            if f.read().startswith('ec2'):
                return True, "instancia EC2 con rol IAM"
    
    return False, None

def run_command(command, description, accept_warnings=False):
    """
    Ejecuta un comando y muestra su salida en tiempo real
    
    Args:
        command: Lista con el comando y sus argumentos (solo strings, no None)
        description: Descripción del paso
        accept_warnings: Si es True, considera éxito incluso con advertencias
        
    Returns:
        Tupla con (éxito, salida_completa)
    """
    # Filtrar cualquier argumento None para evitar errores
    filtered_command = [str(arg) for arg in command if arg is not None]
    
    print(f"\n📋 Ejecutando: {description}")
    print(f"Comando: {' '.join(filtered_command)}")
    print("-" * 70)
    
    output_lines = []
    success = False
    has_warnings = False
    has_downloads = False
    
    try:
        # Ejecutar proceso y capturar salida en tiempo real
        process = subprocess.Popen(
            filtered_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        
        # Mostrar salida línea por línea y capturarla
        for line in process.stdout:
            print(line, end='')
            output_lines.append(line)
            
            # Detectar advertencias en la salida
            if "advertencia" in line.lower() or "warning" in line.lower():
                has_warnings = True
            
            # Detectar si hubo descargas exitosas
            if "descargado" in line.lower() or "downloaded" in line.lower():
                has_downloads = True
        
        # Esperar a que el proceso termine y obtener código de salida
        exit_code = process.wait()
        
        # Evaluar resultado
        if exit_code == 0:
            print(f"\n✅ {description} completado exitosamente")
            success = True
        elif accept_warnings and exit_code == 1 and has_warnings and has_downloads:
            print(f"\n⚠️ {description} completado con advertencias, continuando con el pipeline")
            success = True
        else:
            print(f"\n❌ {description} falló con código de salida {exit_code}")
            success = False
        
        return success, "".join(output_lines)
            
    except Exception as e:
        error_message = f"Error al ejecutar {description}: {e}"
        print(f"\n❌ {error_message}")
        return False, error_message

def check_downloaded_files():
    """
    Verifica si hay archivos descargados en el directorio de datos
    
    Returns:
        True si hay archivos descargados, False si está vacío
    """
    # Rutas comunes donde podrían estar los datos
    data_paths = [
        Path("data/raw"),
        Path("../data/raw"),
        Path("../../data/raw"),
        Path("/Users/andresariasmedina/Documents/HackaDataFusion/data/raw")
    ]
    
    for path in data_paths:
        if path.exists():
            # Buscar archivos .json.gz recursivamente
            files = list(path.glob("**/*.json.gz"))
            if files:
                print(f"\n📊 Encontrados {len(files)} archivos descargados en {path}")
                return True
    
    print("\n❌ No se encontraron archivos descargados")
    return False

def main():
    """Función principal del pipeline"""
    args = parse_arguments()
    
    # Verificar bucket si se va a subir a S3
    if not args.skip_s3 and not args.bucket:
        print("\n❌ Error: Se requiere un bucket de S3 para la subida.")
        print("   Especifique un bucket con --bucket o configure la variable de entorno AWS_S3_BUCKET")
        print("   Alternativamente, use --skip-s3 para omitir la subida a S3")
        return 1
    
    # Obtener rutas a los scripts
    current_dir = Path(__file__).resolve().parent
    
    # Buscar el script de descarga
    download_script = Path("/Users/andresariasmedina/Documents/HackaDataFusion/src/data_flow/download/download.py")
    if not download_script.exists():
        # Intentar con rutas relativas
        alternate_paths = [
            current_dir / "download" / "download.py",
            current_dir / "../download" / "download.py",
            current_dir / "../../data_flow/download" / "download.py",
            Path("src/data_flow/download/download.py")
        ]
        
        for path in alternate_paths:
            if path.exists():
                download_script = path
                break
    
    if not download_script.exists():
        print(f"❌ Error: No se pudo encontrar el script de descarga")
        return 1
    
    # Paso 1: Descargar datos
    download_cmd = [
        sys.executable,
        str(download_script),
        "--start-date", args.start_date,
        "--end-date", args.end_date,
        "--max-workers", "5",
        "--retry-attempts", "3"
    ]
    
    download_success, download_output = run_command(
        download_cmd, 
        "Descarga de datos de GitHub Archive",
        accept_warnings=True  # Aceptar advertencias en la descarga
    )
    
    # Verificar si debemos continuar con la subida a S3
    if args.skip_s3:
        print("\n✅ Pipeline completado (solo descarga)")
        return 0 if download_success else 1
    
    # Verificar si hay archivos para subir
    should_continue = download_success
    
    # Si la descarga falló pero se forzó a continuar, verificar si hay archivos
    if not should_continue and args.force_continue:
        print("\n⚠️ La descarga falló, pero se forzó la continuación")
        should_continue = check_downloaded_files()
    
    if not should_continue:
        print("\n❌ No se puede continuar con el pipeline sin archivos descargados")
        return 1
    
    # Verificar credenciales de AWS antes de intentar la subida
    has_aws_credentials, auth_method = validate_aws_credentials()
    
    if not has_aws_credentials:
        print("\n❌ Error: No se encontraron credenciales de AWS para la subida a S3")
        print("   Opciones:")
        print("   1. Configure las variables de entorno AWS_ACCESS_KEY_ID y AWS_SECRET_ACCESS_KEY")
        print("   2. Configure un perfil de AWS con --profile o la variable AWS_PROFILE")
        print("   3. Use --skip-s3 para omitir la subida a S3")
        return 1
    
    print(f"\n🔑 Utilizando credenciales de AWS desde: {auth_method}")
    
    # Buscar el script de subida a S3
    s3_script = Path("/Users/andresariasmedina/Documents/HackaDataFusion/src/data_flow/storage/s3_upload.py")
    if not s3_script.exists():
        # Intentar con rutas relativas
        alternate_paths = [
            current_dir / "storage" / "s3_upload.py",
            current_dir / "../storage" / "s3_upload.py",
            current_dir / "../../data_flow/storage" / "s3_upload.py",
            Path("src/data_flow/storage/s3_upload.py")
        ]
        
        for path in alternate_paths:
            if path.exists():
                s3_script = path
                break
    
    if not s3_script.exists():
        print(f"❌ Error: No se pudo encontrar el script de subida a S3")
        return 1
    
    # Paso 2: Subir a S3
    s3_cmd = [
        sys.executable,
        str(s3_script),
        "--bucket", args.bucket,
        "--prefix", args.prefix,
        "--region", args.region
    ]
    
    # Añadir perfil solo si está definido
    if args.profile:
        s3_cmd.extend(["--profile", args.profile])
    
    s3_success, s3_output = run_command(s3_cmd, "Subida de datos a S3")
    
    # Determinar resultado final
    if download_success and s3_success:
        print("\n✅ Pipeline completado exitosamente")
        status_code = 0
    elif s3_success:
        print("\n⚠️ Pipeline completado con advertencias en la descarga")
        status_code = 1
    else:
        print("\n❌ Pipeline falló en la subida a S3")
        status_code = 2
    
    print(f"📊 Datos procesados desde {args.start_date} hasta {args.end_date}")
    print(f"📦 Bucket destino: s3://{args.bucket}/{args.prefix}")
    
    return status_code

if __name__ == "__main__":
    sys.exit(main())