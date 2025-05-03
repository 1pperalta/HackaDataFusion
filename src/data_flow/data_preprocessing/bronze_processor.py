#!/usr/bin/env python
"""
GitHub Archive Bronze Processor (CSV Version)

Este script procesa los datos crudos de GitHub Archive (capa Raw) 
y los prepara para la capa Bronze en formato CSV, manteniendo
todos los datos originales pero estructurándolos adecuadamente.

La capa Bronze mantiene los datos sin filtrar pero en un formato
más adecuado para su procesamiento.
"""

import os
import sys
import logging
import argparse
import datetime
import gzip
import json
import glob
from pathlib import Path
from typing import List, Dict, Any, Optional, Generator
import time
import hashlib

import pandas as pd
from tqdm import tqdm

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/bronze_processor.log", mode='a')
    ]
)
logger = logging.getLogger("bronze_processor")

# Directorios para datos
RAW_DATA_DIR = Path("data/raw")
BRONZE_DIR = Path("data/processed/bronze")


class BronzeProcessor:
    """Procesador de capa Bronze para eventos de GitHub"""
    
    def __init__(
        self,
        raw_dir: Path = RAW_DATA_DIR,
        bronze_dir: Path = BRONZE_DIR,
        batch_size: int = 10000,
        use_csv: bool = True
    ):
        """
        Inicializa el procesador de capa Bronze
        
        Args:
            raw_dir: Directorio con datos crudos
            bronze_dir: Directorio para datos de capa Bronze
            batch_size: Tamaño de lote para procesar eventos
            use_csv: Si se debe usar CSV en lugar de parquet (en caso de error)
        """
        self.raw_dir = raw_dir
        self.bronze_dir = bronze_dir
        self.batch_size = batch_size
        self.use_csv = use_csv
        
        # Crear directorio bronze si no existe
        self.bronze_dir.mkdir(parents=True, exist_ok=True)
    
    def find_raw_files(self, date_pattern: Optional[str] = None) -> List[Path]:
        """
        Encuentra archivos en el directorio de datos crudos
        
        Args:
            date_pattern: Patrón de fecha opcional (YYYY/MM/DD)
            
        Returns:
            Lista de rutas a archivos .json.gz
        """
        if date_pattern:
            search_pattern = f"{self.raw_dir}/{date_pattern}/**/*.json.gz"
        else:
            search_pattern = f"{self.raw_dir}/**/*.json.gz"
        
        files = glob.glob(search_pattern, recursive=True)
        return [Path(file) for file in files]
    
    def read_gzip_json_file(self, file_path: Path) -> Generator[Dict[str, Any], None, None]:
        """
        Lee un archivo gzip con JSON por línea
        
        Args:
            file_path: Ruta al archivo
            
        Yields:
            Cada línea del archivo como un diccionario
        """
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for line in f:
                try:
                    yield json.loads(line.strip())
                except json.JSONDecodeError:
                    logger.warning(f"Error decodificando JSON en archivo {file_path}")
                    continue
    
    def process_file_to_bronze(self, file_path: Path) -> pd.DataFrame:
        """
        Procesa un archivo y extrae datos para la capa Bronze
        
        Args:
            file_path: Ruta al archivo
            
        Returns:
            DataFrame con datos procesados para capa Bronze
        """
        events = []
        file_name = file_path.name
        file_date_str = file_name.split(".")[0]  # '2025-05-01-15'
        
        # Extraer partes de la fecha
        try:
            year, month, day, hour = file_date_str.split("-")
            date_hour = f"{year}-{month}-{day} {hour}:00:00"
        except ValueError:
            logger.warning(f"Formato de nombre de archivo inesperado: {file_name}")
            date_hour = datetime.datetime.now().strftime("%Y-%m-%d %H:00:00")
        
        for event in self.read_gzip_json_file(file_path):
            # Generar ID único para el evento
            event_hash = hashlib.md5(json.dumps(event, sort_keys=True).encode()).hexdigest()
            
            # Preparar evento para Bronze
            bronze_event = {
                # Metadatos del archivo y procesamiento
                'event_hash': event_hash,
                'file_name': file_name,
                'file_date': file_date_str,
                'processed_at': datetime.datetime.now().isoformat(),
                'hour_bucket': date_hour,
                
                # Datos del evento en formato JSON
                'raw_data': json.dumps(event)
            }
            
            # Extraer campos principales para buscar/filtrar
            if 'id' in event:
                bronze_event['event_id'] = event['id']
            
            if 'type' in event:
                bronze_event['event_type'] = event['type']
            
            if 'created_at' in event:
                bronze_event['created_at'] = event['created_at']
            
            if 'actor' in event and isinstance(event['actor'], dict):
                if 'id' in event['actor']:
                    bronze_event['actor_id'] = event['actor']['id']
                if 'login' in event['actor']:
                    bronze_event['actor_login'] = event['actor']['login']
            
            if 'repo' in event and isinstance(event['repo'], dict):
                if 'id' in event['repo']:
                    bronze_event['repo_id'] = event['repo']['id']
                if 'name' in event['repo']:
                    bronze_event['repo_name'] = event['repo']['name']
            
            if 'payload' in event:
                bronze_event['payload'] = json.dumps(event['payload'])
            
            events.append(bronze_event)
            
            # Procesar en lotes para controlar uso de memoria
            if len(events) >= self.batch_size:
                df = pd.DataFrame(events)
                events = []
                yield df
        
        # Procesar eventos restantes
        if events:
            df = pd.DataFrame(events)
            yield df
    
    def save_bronze_data(self, df: pd.DataFrame, date_str: str) -> Path:
        """
        Guarda datos de capa Bronze en formato parquet o CSV
        
        Args:
            df: DataFrame con datos
            date_str: Fecha en formato YYYY-MM-DD-HH
            
        Returns:
            Ruta al archivo guardado
        """
        # Crear estructura de directorios por fecha
        try:
            year, month, day, hour = date_str.split("-")
            output_dir = self.bronze_dir / year / month / day
        except ValueError:
            logger.warning(f"Formato de fecha inesperado: {date_str}")
            # Usar la fecha actual si no se puede parsear
            now = datetime.datetime.now()
            year, month, day = now.strftime("%Y,%m,%d").split(",")
            hour = now.strftime("%H")
            output_dir = self.bronze_dir / year / month / day
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Intentar guardar como parquet si no se especifica usar CSV
        if not self.use_csv:
            try:
                output_path = output_dir / f"{date_str}.parquet"
                df.to_parquet(output_path, index=False)
                return output_path
            except ImportError as e:
                logger.warning(f"Error al guardar como parquet: {e}")
                logger.warning("Cambiando a formato CSV")
                self.use_csv = True  # Cambiar a CSV para futuros archivos
        
        # Guardar como CSV
        output_path = output_dir / f"{date_str}.csv"
        df.to_csv(output_path, index=False)
        return output_path
    
    def process_file(self, file_path: Path) -> Dict[str, Any]:
        """
        Procesa un archivo completo
        
        Args:
            file_path: Ruta al archivo
            
        Returns:
            Diccionario con estadísticas de procesamiento
        """
        try:
            logger.info(f"Procesando archivo: {file_path}")
            
            file_name = file_path.name
            date_str = file_name.split(".")[0]  # '2025-05-01-15'
            
            # Verificar si ya existe el archivo procesado
            try:
                year, month, day, hour = date_str.split("-")
                output_dir = self.bronze_dir / year / month / day
                
                # Verificar si existe alguna versión (parquet o csv)
                parquet_path = output_dir / f"{date_str}.parquet"
                csv_path = output_dir / f"{date_str}.csv"
                
                if parquet_path.exists() or csv_path.exists():
                    existing_path = parquet_path if parquet_path.exists() else csv_path
                    logger.info(f"Archivo ya procesado: {existing_path}, omitiendo")
                    return {
                        "file": str(file_path),
                        "output": str(existing_path),
                        "status": "skipped",
                        "rows": 0,
                        "error": None
                    }
            except (ValueError, FileNotFoundError):
                pass  # Continuar con el procesamiento
            
            # Procesar archivo
            total_rows = 0
            saved_path = None
            
            for batch_df in self.process_file_to_bronze(file_path):
                total_rows += len(batch_df)
                saved_path = self.save_bronze_data(batch_df, date_str)
            
            logger.info(f"Archivo {file_path} procesado exitosamente: {total_rows} eventos")
            return {
                "file": str(file_path),
                "output": str(saved_path) if saved_path else None,
                "status": "success",
                "rows": total_rows,
                "error": None
            }
            
        except Exception as e:
            logger.error(f"Error procesando archivo {file_path}: {e}", exc_info=True)
            return {
                "file": str(file_path),
                "output": None,
                "status": "error",
                "rows": 0,
                "error": str(e)
            }
    
    def run(self, date_pattern: Optional[str] = None, max_workers: int = 1) -> Dict[str, Any]:
        """
        Ejecuta el procesamiento de todos los archivos
        
        Args:
            date_pattern: Patrón de fecha opcional (YYYY/MM/DD)
            max_workers: Número máximo de trabajadores paralelos
            
        Returns:
            Estadísticas del procesamiento
        """
        # Encontrar archivos
        files = self.find_raw_files(date_pattern)
        
        if not files:
            logger.warning(f"No se encontraron archivos para procesar")
            return {
                "processed_files": 0,
                "successful_files": 0,
                "failed_files": 0,
                "total_rows": 0,
                "files": []
            }
        
        logger.info(f"Se encontraron {len(files)} archivos para procesar")
        
        # Procesar archivos
        results = []
        
        with tqdm(total=len(files), desc="Procesando archivos") as pbar:
            for file_path in files:
                result = self.process_file(file_path)
                results.append(result)
                pbar.update(1)
        
        # Estadísticas
        successful = [r for r in results if r["status"] == "success"]
        failed = [r for r in results if r["status"] == "error"]
        skipped = [r for r in results if r["status"] == "skipped"]
        
        total_rows = sum(r["rows"] for r in results)
        
        stats = {
            "processed_files": len(files),
            "successful_files": len(successful),
            "failed_files": len(failed),
            "skipped_files": len(skipped),
            "total_rows": total_rows,
            "files": results
        }
        
        logger.info(f"Procesamiento completado: {len(successful)}/{len(files)} archivos exitosos, {total_rows} eventos")
        
        return stats


def parse_arguments():
    """
    Parsea los argumentos de línea de comandos
    
    Returns:
        Argumentos parseados
    """
    parser = argparse.ArgumentParser(description="Procesador de capa Bronze para datos de GitHub Archive")
    
    # Argumentos para procesamiento
    parser.add_argument(
        "--date",
        type=str,
        help="Fecha específica a procesar en formato YYYY/MM/DD"
    )
    
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        help="Tamaño de lote para procesar eventos (default: 10000)"
    )
    
    parser.add_argument(
        "--use-csv",
        action="store_true",
        help="Usar formato CSV en lugar de parquet"
    )
    
    parser.add_argument(
        "--max-workers",
        type=int,
        default=1,
        help="Número máximo de trabajadores paralelos (default: 1)"
    )
    
    return parser.parse_args()


def main():
    """Función principal"""
    try:
        # Crear directorio de logs si no existe
        Path("logs").mkdir(exist_ok=True)
        
        # Parsear argumentos
        args = parse_arguments()
        
        # Inicializar procesador
        processor = BronzeProcessor(
            batch_size=args.batch_size,
            use_csv=args.use_csv
        )
        
        # Ejecutar procesamiento
        stats = processor.run(
            date_pattern=args.date,
            max_workers=args.max_workers
        )
        
        # Mostrar resultados
        print(f"\n✅ Procesamiento completado:")
        print(f"📊 {stats['successful_files']}/{stats['processed_files']} archivos procesados correctamente")
        print(f"⚠️ {stats['failed_files']} archivos fallidos")
        print(f"📝 {stats['skipped_files']} archivos omitidos (ya procesados)")
        print(f"🔢 Total de eventos procesados: {stats['total_rows']}")
        
        return 0
        
    except Exception as e:
        logger.critical(f"Error en el procesamiento: {e}", exc_info=True)
        print(f"\n❌ Error en el procesamiento: {e}")
        return 2


if __name__ == "__main__":
    sys.exit(main())