#!/usr/bin/env python
"""
GitHub Archive Downloader Mejorado

Este script descarga datos de eventos de GitHub desde gharchive.org
y los almacena en la carpeta data/raw del proyecto.

Características:
- Verificación previa de disponibilidad de archivos
- Descarga paralela de archivos para mejor rendimiento
- Manejo inteligente de archivos no disponibles
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
from typing import List, Dict, Tuple, Optional, Set
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
    """Descargador de archivos de GitHub Archive con verificación previa de disponibilidad"""
    
    def __init__(
        self, 
        start_date: datetime.datetime, 
        end_date: datetime.datetime, 
        output_dir: Path = RAW_DATA_DIR,
        max_workers: int = 5,
        retry_attempts: int = 3,
        retry_delay: int = 5,
        check_availability: bool = True
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
            check_availability: Si se debe verificar disponibilidad antes de descargar
        """
        self.start_date = start_date
        self.end_date = end_date
        self.output_dir = output_dir
        self.max_workers = max_workers
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        self.check_availability = check_availability
        
        # Crear directorio de salida si no existe
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Validar fechas
        if self.end_date <= self.start_date:
            raise ValueError("La fecha de fin debe ser posterior a la fecha de inicio")
        
        if (self.end_date - self.start_date).days > 7:
            raise ValueError("El período máximo permitido para el hackathon es de 7 días")
    
    def check_file_availability(self, url: str) -> bool:
        """
        Verifica si un archivo está disponible sin descargarlo completamente
        
        Args:
            url: URL del archivo a verificar
            
        Returns:
            True si el archivo está disponible, False en caso contrario
        """
        try:
            # Realizar solicitud HEAD para verificar disponibilidad
            response = requests.head(url, timeout=5)
            return response.status_code == 200
        except requests.RequestException as e:
            logger.warning(f"Error al verificar disponibilidad de {url}: {e}")
            return False
    
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
            
            # Si la verificación de disponibilidad está activada, comprobar primero
            if self.check_availability:
                if self.check_file_availability(url):
                    urls.append((url, target_path))
                else:
                    logger.info(f"Archivo {url} no está disponible, omitiendo")
            else:
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
        
        # Si no hay archivos disponibles
        if total_hours == 0:
            logger.warning("No se encontraron archivos disponibles para descargar")
            return results
        
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
    
    def get_available_dates(self) -> Set[datetime.date]:
        """
        Obtiene las fechas disponibles en los archivos descargados
        
        Returns:
            Conjunto de fechas disponibles
        """
        dates = set()
        
        for root, _, files in os.walk(self.output_dir):
            for file in files:
                if file.endswith(".json.gz"):
                    try:
                        # Extraer fecha del nombre del archivo
                        date_str = file.split(".")[0][:10]  # '2025-05-01'
                        date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
                        dates.add(date)
                    except (ValueError, IndexError):
                        continue
        
        return dates
    
    def scan_available_pattern(self) -> Dict[str, List[int]]:
        """
        Analiza el patrón de disponibilidad de los archivos
        
        Returns:
            Diccionario con {fecha: [horas_disponibles]}
        """
        availability = {}
        
        # Primero intentar buscar archivos descargados
        for root, _, files in os.walk(self.output_dir):
            for file in files:
                if file.endswith(".json.gz"):
                    try:
                        # Extraer fecha y hora del nombre del archivo
                        date_hour = file.split(".")[0]  # '2025-05-01-10'
                        date_str = date_hour[:10]  # '2025-05-01'
                        hour = int(date_hour[11:13])  # 10
                        
                        if date_str not in availability:
                            availability[date_str] = []
                        
                        availability[date_str].append(hour)
                    except (ValueError, IndexError):
                        continue
        
        # Si no hay archivos descargados, verificar disponibilidad en línea
        if not availability:
            current_date = self.start_date
            while current_date < self.end_date:
                date_str = current_date.strftime('%Y-%m-%d')
                availability[date_str] = []
                
                for hour in range(24):
                    file_name = f"{date_str}-{hour:02d}.json.gz"
                    url = f"{GH_ARCHIVE_BASE_URL}/{file_name}"
                    
                    if self.check_file_availability(url):
                        availability[date_str].append(hour)
                
                current_date += datetime.timedelta(days=1)
        
        return availability


def parse_arguments():
    """
    Parsea los argumentos de línea de comandos
    
    Returns:
        Argumentos parseados
    """
    parser = argparse.ArgumentParser(description="Descargador mejorado de datos de GitHub Archive")
    
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
    
    parser.add_argument(
        "--check-availability",
        action="store_true",
        help="Verificar disponibilidad de archivos antes de descargar"
    )
    
    parser.add_argument(
        "--analyze-only",
        action="store_true",
        help="Solo analizar disponibilidad sin descargar"
    )
    
    parser.add_argument(
        "--smart-hours",
        action="store_true",
        help="Descargar solo horas disponibles según patrón observado (10-23)"
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
        
        # Inicializar descargador
        downloader = GitHubArchiveDownloader(
            start_date=args.start_date,
            end_date=args.end_date,
            max_workers=args.max_workers,
            retry_attempts=args.retry_attempts,
            retry_delay=args.retry_delay,
            check_availability=args.check_availability
        )
        
        # Si solo queremos analizar disponibilidad
        if args.analyze_only:
            print("\n🔍 Analizando disponibilidad de archivos...")
            availability = downloader.scan_available_pattern()
            
            for date_str, hours in availability.items():
                hours.sort()
                print(f"📅 {date_str}: {len(hours)} horas disponibles")
                print(f"  ⏰ Horas: {', '.join(f'{h:02d}' for h in hours)}")
            
            print("\n📊 Patrón de disponibilidad detectado:")
            
            # Detectar patrón
            pattern = {}
            for date_str, hours in availability.items():
                if hours:
                    min_hour = min(hours)
                    max_hour = max(hours)
                    print(f"  📅 {date_str}: Archivos disponibles desde hora {min_hour:02d} hasta {max_hour:02d}")
                    pattern[date_str] = (min_hour, max_hour)
                else:
                    print(f"  📅 {date_str}: No hay archivos disponibles")
            
            return 0
        
        # Si queremos usar solo las horas que sabemos disponibles
        if args.smart_hours:
            # Modificar el rango de horas para solo descargar 10-23 cada día
            modified_start = args.start_date.replace(hour=10)
            modified_end = args.end_date
            
            # Si la fecha de inicio es hoy, ajustar la hora final
            if modified_start.date() == modified_end.date():
                # Solo descargar hasta la hora actual
                current_hour = datetime.datetime.now().hour
                end_hour = min(23, current_hour)
                modified_end = args.end_date.replace(hour=end_hour)
            
            # Crear nuevo descargador con fechas modificadas
            downloader = GitHubArchiveDownloader(
                start_date=modified_start,
                end_date=modified_end,
                max_workers=args.max_workers,
                retry_attempts=args.retry_attempts,
                retry_delay=args.retry_delay,
                check_availability=args.check_availability
            )
            
            print(f"\n⚙️ Usando modo inteligente de descarga:")
            print(f"   Descargando solo horas 10-23 de cada día del {modified_start.strftime('%Y-%m-%d')} al {modified_end.strftime('%Y-%m-%d')}")
        
        # Ejecutar descarga
        results = downloader.run()
        
        # Obtener estadísticas
        stats = downloader.get_download_stats()
        logger.info(f"Estadísticas de descarga: {stats}")
        
        # Informar resultados
        successful = sum(1 for success in results.values() if success)
        total = len(results)
        
        if successful == total and total > 0:
            logger.info(f"Descarga exitosa: {successful}/{total} archivos")
            print(f"\n✅ Descarga completada exitosamente: {successful}/{total} archivos")
            print(f"📊 Total descargado: {stats['total_size_mb']:.2f} MB en {stats['total_files']} archivos")
            print(f"📂 Archivos guardados en: {RAW_DATA_DIR}")
            return 0
        elif successful > 0:
            logger.warning(f"Descarga completada con advertencias: {successful}/{total} archivos")
            failed_files = [str(path) for path, success in results.items() if not success]
            logger.warning(f"Archivos fallidos: {failed_files}")
            print(f"\n⚠️ Descarga completada con advertencias: {successful}/{total} archivos")
            print(f"❌ {total - successful} archivos fallaron, ver logs para detalles")
            print(f"📊 Total descargado: {stats['total_size_mb']:.2f} MB en {stats['total_files']} archivos")
            return 1
        else:
            logger.error("No se pudieron descargar archivos")
            print("\n❌ No se pudieron descargar archivos")
            if args.check_availability:
                print("💡 Intente usar --analyze-only para verificar disponibilidad")
            else:
                print("💡 Intente usar --check-availability para verificar disponibilidad primero")
            return 2
    
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