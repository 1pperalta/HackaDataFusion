#!/usr/bin/env python
"""
GitHub Archive Downloader

Este script descarga datos de eventos de GitHub desde gharchive.org
y los almacena en la carpeta data/raw del proyecto.

Características:
- Descarga paralela de archivos para mejor rendimiento
- Manejo de errores y reintentos
- Verificación de integridad de los archivos
- Soporte para rangos de fechas personalizados
"""

import os
import sys
import argparse
import logging
import datetime
import gzip
import json
import concurrent.futures
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import hashlib
import time

import requests
from tqdm import tqdm

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/github_downloader.log", mode='a')
    ]
)
logger = logging.getLogger("github_downloader")

# URL base de GitHub Archive
GH_ARCHIVE_BASE_URL = "https://data.gharchive.org"

# Directorio para almacenar los datos
RAW_DATA_DIR = Path("data/raw")

class GitHubArchiveDownloader:
    """Descargador de archivos de GitHub Archive"""
    
    def __init__(
        self, 
        start_date: datetime.datetime, 
        end_date: datetime.datetime, 
        output_dir: Path = RAW_DATA_DIR,
        max_workers: int = 5,
        retry_attempts: int = 3,
        retry_delay: int = 5
    ):
        """
        Inicializa el descargador de GitHub Archive
        
        Args:
            start_date: Fecha de inicio para descargar datos
            end_date: Fecha de fin para descargar datos
            output_dir: Directorio donde guardar los archivos descargados
            max_workers: Número máximo de trabajadores para descargas paralelas
            retry_attempts: Número de intentos de reintento para descargas fallidas
            retry_delay: Tiempo de espera entre reintentos (segundos)
        """
        self.start_date = start_date
        self.end_date = end_date
        self.output_dir = output_dir
        self.max_workers = max_workers
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        
        # Crear directorio de salida si no existe
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Validar fechas
        if self.end_date <= self.start_date:
            raise ValueError("La fecha de fin debe ser posterior a la fecha de inicio")
        
        if (self.end_date - self.start_date).days > 7:
            raise ValueError("El período máximo permitido para el hackathon es de 7 días")
    
    def generate_hour_urls(self) -> List[Tuple[str, Path]]:
        """
        Genera URLs para cada hora en el rango de fechas
        
        Returns:
            Lista de tuplas con (URL, ruta_destino)
        """
        urls = []
        
        current_date = self.start_date
        while current_date < self.end_date:
            # Formato de archivo: YYYY-MM-DD-H.json.gz
            file_name = f"{current_date.strftime('%Y-%m-%d-%H')}.json.gz"
            url = f"{GH_ARCHIVE_BASE_URL}/{file_name}"
            
            # Crear estructura de directorios por fecha
            date_dir = self.output_dir / current_date.strftime('%Y/%m/%d')
            date_dir.mkdir(parents=True, exist_ok=True)
            
            target_path = date_dir / file_name
            
            urls.append((url, target_path))
            current_date += datetime.timedelta(hours=1)
        
        return urls
    
    def download_file(self, url: str, target_path: Path) -> bool:
        """
        Descarga un archivo con reintentos
        
        Args:
            url: URL del archivo a descargar
            target_path: Ruta donde guardar el archivo
            
        Returns:
            True si la descarga fue exitosa, False en caso contrario
        """
        # Si el archivo ya existe y es válido, omitir descarga
        if target_path.exists() and self._validate_file(target_path):
            logger.info(f"Archivo {target_path} ya existe y es válido, omitiendo")
            return True
        
        # Crear directorio padre si no existe
        target_path.parent.mkdir(parents=True, exist_ok=True)
        
        for attempt in range(self.retry_attempts):
            try:
                response = requests.get(url, stream=True)
                response.raise_for_status()
                
                # Obtener tamaño total para mostrar progreso
                total_size = int(response.headers.get("content-length", 0))
                
                # Descargar con barra de progreso
                with open(target_path, "wb") as f:
                    with tqdm(
                        total=total_size,
                        unit="B",
                        unit_scale=True,
                        desc=target_path.name,
                        disable=not sys.stdout.isatty()
                    ) as pbar:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                pbar.update(len(chunk))
                
                return self._validate_file(target_path)
            
            except requests.RequestException as e:
                logger.warning(f"Intento {attempt+1}/{self.retry_attempts} fallido para {url}: {e}")
                if attempt < self.retry_attempts - 1:
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"Error al descargar {url} después de {self.retry_attempts} intentos")
                    # Eliminar archivo parcial si existe
                    if target_path.exists():
                        target_path.unlink()
                    return False
    
    def _validate_file(self, file_path: Path) -> bool:
        """
        Valida que el archivo descargado sea un archivo gzip válido con JSON válido
        
        Args:
            file_path: Ruta del archivo a validar
            
        Returns:
            True si el archivo es válido, False en caso contrario
        """
        try:
            with gzip.open(file_path, "rb") as f:
                # Leer solo las primeras líneas para validar formato
                for i, line in enumerate(f):
                    if i >= 5:  # Validar solo las primeras 5 líneas
                        break
                    json.loads(line)
            return True
        except (gzip.BadGzipFile, json.JSONDecodeError) as e:
            logger.error(f"Archivo inválido {file_path}: {e}")
            return False
    
    def process_url(self, url_info: Tuple[str, Path]) -> Tuple[Path, bool]:
        """
        Procesa una URL: descarga el archivo
        
        Args:
            url_info: Tupla con (URL, ruta_destino)
            
        Returns:
            Tupla con (ruta_destino, éxito)
        """
        url, target_path = url_info
        
        logger.info(f"Descargando {url} a {target_path}")
        success = self.download_file(url, target_path)
        
        return target_path, success
    
    def run(self) -> Dict[Path, bool]:
        """
        Ejecuta el proceso de descarga paralela
        
        Returns:
            Diccionario con {ruta_destino: éxito}
        """
        urls = self.generate_hour_urls()
        results = {}
        
        total_hours = len(urls)
        logger.info(f"Descargando {total_hours} archivos de GitHub Archive")
        logger.info(f"Período: {self.start_date} a {self.end_date}")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_url = {executor.submit(self.process_url, url_info): url_info for url_info in urls}
            
            with tqdm(
                total=total_hours, 
                desc="Progreso global", 
                unit="archivos",
                disable=not sys.stdout.isatty()
            ) as pbar:
                for future in concurrent.futures.as_completed(future_to_url):
                    url_info = future_to_url[future]
                    try:
                        target_path, success = future.result()
                        results[target_path] = success
                    except Exception as e:
                        logger.error(f"Error al procesar {url_info[0]}: {e}")
                        results[url_info[1]] = False
                    pbar.update(1)
        
        # Resultados
        successful = sum(1 for success in results.values() if success)
        logger.info(f"Descarga completada: {successful}/{total_hours} archivos exitosos")
        
        return results
    
    def get_download_stats(self) -> Dict[str, int]:
        """
        Obtiene estadísticas de los archivos descargados
        
        Returns:
            Diccionario con estadísticas
        """
        total_size = 0
        total_files = 0
        
        for root, _, files in os.walk(self.output_dir):
            for file in files:
                if file.endswith(".json.gz"):
                    file_path = Path(root) / file
                    total_size += file_path.stat().st_size
                    total_files += 1
        
        return {
            "total_files": total_files,
            "total_size_bytes": total_size,
            "total_size_mb": total_size / (1024 * 1024)
        }


def parse_arguments():
    """
    Parsea los argumentos de línea de comandos
    
    Returns:
        Argumentos parseados
    """
    parser = argparse.ArgumentParser(description="Descargador de datos de GitHub Archive")
    
    # Argumentos para fechas
    parser.add_argument(
        "--start-date", 
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d"),
        default=(datetime.datetime.now() - datetime.timedelta(days=2)).strftime("%Y-%m-%d"),
        help="Fecha de inicio en formato YYYY-MM-DD (default: hace 2 días)"
    )
    
    parser.add_argument(
        "--end-date", 
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d"),
        default=datetime.datetime.now().strftime("%Y-%m-%d"),
        help="Fecha de fin en formato YYYY-MM-DD (default: hoy)"
    )
    
    # Argumentos de configuración
    parser.add_argument(
        "--max-workers", 
        type=int, 
        default=5,
        help="Número máximo de trabajadores para descargas paralelas (default: 5)"
    )
    
    parser.add_argument(
        "--retry-attempts", 
        type=int, 
        default=3,
        help="Número de intentos de reintento para descargas fallidas (default: 3)"
    )
    
    parser.add_argument(
        "--retry-delay", 
        type=int, 
        default=5,
        help="Tiempo de espera entre reintentos en segundos (default: 5)"
    )
    
    args = parser.parse_args()
    
    # Convertir fechas a datetime con hora
    args.start_date = datetime.datetime.combine(args.start_date, datetime.time(0, 0))
    args.end_date = datetime.datetime.combine(args.end_date, datetime.time(0, 0))
    
    return args


def main():
    """Función principal"""
    try:
        # Crear directorio de logs si no existe
        Path("logs").mkdir(exist_ok=True)
        
        # Parsear argumentos
        args = parse_arguments()
        
        # Inicializar y ejecutar descargador
        downloader = GitHubArchiveDownloader(
            start_date=args.start_date,
            end_date=args.end_date,
            max_workers=args.max_workers,
            retry_attempts=args.retry_attempts,
            retry_delay=args.retry_delay
        )
        
        results = downloader.run()
        
        # Obtener estadísticas
        stats = downloader.get_download_stats()
        logger.info(f"Estadísticas de descarga: {stats}")
        
        # Informar resultados
        successful = sum(1 for success in results.values() if success)
        total = len(results)
        
        if successful == total:
            logger.info(f"Descarga exitosa: {successful}/{total} archivos")
            print(f"\n✅ Descarga completada exitosamente: {successful}/{total} archivos")
            print(f"📊 Total descargado: {stats['total_size_mb']:.2f} MB en {stats['total_files']} archivos")
            print(f"📂 Archivos guardados en: {RAW_DATA_DIR}")
            return 0
        else:
            logger.warning(f"Descarga completada con errores: {successful}/{total} archivos")
            failed_files = [str(path) for path, success in results.items() if not success]
            logger.warning(f"Archivos fallidos: {failed_files}")
            print(f"\n⚠️ Descarga completada con advertencias: {successful}/{total} archivos")
            print(f"❌ {total - successful} archivos fallaron, ver logs para detalles")
            print(f"📊 Total descargado: {stats['total_size_mb']:.2f} MB en {stats['total_files']} archivos")
            return 1
    
    except KeyboardInterrupt:
        logger.info("Descarga interrumpida por el usuario")
        print("\n⚠️ Descarga interrumpida por el usuario")
        return 130
    
    except Exception as e:
        logger.critical(f"Error en la descarga: {e}", exc_info=True)
        print(f"\n❌ Error en la descarga: {e}")
        return 2


if __name__ == "__main__":
    sys.exit(main())