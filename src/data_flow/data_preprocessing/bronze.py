#!/usr/bin/env python
"""
GitHub Archive Bronze Processing

Este script procesa los archivos json.gz descargados en la carpeta data/raw
y los convierte a un formato más estructurado y optimizado para consultas
en la capa Bronze.

Características:
- Lectura y decompresión de archivos json.gz
- Validación de esquemas y datos
- Conversión a formato columnar (Parquet)
- Extracción de metadatos
- Particionado eficiente
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
from typing import List, Dict, Any, Tuple, Optional
import time

import pandas as pd
import numpy as np
from tqdm import tqdm

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/bronze_processing.log", mode='a')
    ]
)
logger = logging.getLogger("bronze_processor")

# Directorios para raw data y bronze data
RAW_DATA_DIR = Path("data/raw")
BRONZE_DATA_DIR = Path("data/bronze")

# Crear directorio de logs si no existe
Path("logs").mkdir(exist_ok=True)
BRONZE_DATA_DIR.mkdir(parents=True, exist_ok=True)


class BronzeProcessor:
    """Procesador de datos crudos a Bronze para GitHub Archive"""
    
    def __init__(
        self,
        input_dir: Path = RAW_DATA_DIR,
        output_dir: Path = BRONZE_DATA_DIR,
        max_workers: int = 5,
        batch_size: int = 100000,
        preserve_path_structure: bool = True,
        compression: str = 'snappy'
    ):
        """
        Inicializa el procesador de la capa Bronze
        
        Args:
            input_dir: Directorio con archivos raw json.gz
            output_dir: Directorio para guardar archivos bronze parquet
            max_workers: Número máximo de trabajadores para procesamiento paralelo
            batch_size: Número de registros a procesar por lote
            preserve_path_structure: Si se debe preservar la estructura de directorios
            compression: Método de compresión para archivos parquet
        """
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.preserve_path_structure = preserve_path_structure
        self.compression = compression
        
        # Crear directorio de salida si no existe
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def find_raw_files(self) -> List[Path]:
        """
        Encuentra todos los archivos .json.gz en el directorio de entrada
        
        Returns:
            Lista de rutas a archivos
        """
        files = []
        
        for item in self.input_dir.glob('**/*.json.gz'):
            if item.is_file():
                files.append(item)
        
        return files
    
    def get_output_path(self, input_path: Path) -> Path:
        """
        Genera la ruta de salida para un archivo procesado
        
        Args:
            input_path: Ruta del archivo de entrada
            
        Returns:
            Ruta de salida correspondiente
        """
        if self.preserve_path_structure:
            # Mantener la misma estructura de carpetas
            relative_path = input_path.relative_to(self.input_dir)
            output_path = self.output_dir / relative_path.parent / f"{relative_path.stem}.parquet"
        else:
            # Guardar todo en el directorio raíz
            output_path = self.output_dir / f"{input_path.stem}.parquet"
        
        # Asegurarse de que el directorio padre existe
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        return output_path
    
    def process_jsonl_to_df(self, input_path: Path) -> pd.DataFrame:
        """
        Procesa un archivo JSONL comprimido a DataFrame
        
        Args:
            input_path: Ruta del archivo de entrada
            
        Returns:
            DataFrame con datos procesados
        """
        records = []
        
        try:
            with gzip.open(input_path, 'rt', encoding='utf-8') as f:
                for i, line in enumerate(f):
                    try:
                        # Parsear la línea como JSON
                        record = json.loads(line)
                        
                        # Extraer valores de timestamp para particionar
                        if 'created_at' in record:
                            # Convertir a datetime si es posible
                            try:
                                dt = datetime.datetime.fromisoformat(record['created_at'].replace('Z', '+00:00'))
                                record['event_date'] = dt.strftime('%Y-%m-%d')
                                record['event_hour'] = dt.strftime('%H')
                            except (ValueError, TypeError):
                                record['event_date'] = None
                                record['event_hour'] = None
                        
                        records.append(record)
                        
                        # Procesar en lotes para evitar problemas de memoria
                        if len(records) >= self.batch_size:
                            logger.info(f"Procesamiento por lotes: {len(records)} registros")
                            break
                    
                    except json.JSONDecodeError:
                        logger.warning(f"Error al decodificar JSON en la línea {i+1} de {input_path}")
                    except Exception as e:
                        logger.warning(f"Error procesando línea {i+1} de {input_path}: {e}")
        
        except Exception as e:
            logger.error(f"Error al abrir o leer archivo {input_path}: {e}")
            return pd.DataFrame()
        
        # Convertir a DataFrame
        if not records:
            logger.warning(f"No se encontraron registros válidos en {input_path}")
            return pd.DataFrame()
            
        return pd.DataFrame(records)
    
    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpia y normaliza un DataFrame
        
        Args:
            df: DataFrame a limpiar
            
        Returns:
            DataFrame limpio y normalizado
        """
        if df.empty:
            return df
            
        # 1. Convertir columnas de fecha a formato datetime
        date_columns = ['created_at', 'updated_at', 'closed_at', 'merged_at', 'due_on']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # 2. Garantizar presencia de columnas importantes
        if 'id' not in df.columns:
            df['id'] = np.nan
        
        if 'type' not in df.columns:
            df['type'] = 'Unknown'
            
        return df
    
    def process_file(self, input_path: Path) -> Tuple[Path, bool]:
        """
        Procesa un archivo json.gz y lo convierte a parquet
        
        Args:
            input_path: Ruta del archivo de entrada
            
        Returns:
            Tupla con (ruta_salida, éxito)
        """
        output_path = self.get_output_path(input_path)
        
        # Verificar si el archivo ya existe
        if output_path.exists():
            logger.info(f"Archivo {output_path} ya existe, omitiendo")
            return output_path, True
            
        try:
            logger.info(f"Procesando archivo {input_path}")
            
            # Leer y convertir a DataFrame
            df = self.process_jsonl_to_df(input_path)
            
            if df.empty:
                logger.warning(f"No se pudo extraer datos de {input_path}")
                return output_path, False
            
            # Limpiar y normalizar DataFrame
            df = self.clean_dataframe(df)
            
            # Guardar como parquet
            df.to_parquet(
                output_path,
                compression=self.compression,
                index=False
            )
            
            logger.info(f"Archivo {input_path} convertido exitosamente a {output_path}")
            return output_path, True
            
        except Exception as e:
            logger.error(f"Error al procesar {input_path}: {e}", exc_info=True)
            # Si el archivo de salida se creó parcialmente, eliminarlo
            if output_path.exists():
                output_path.unlink()
            return output_path, False
    
    def run(self) -> Dict[Path, bool]:
        """
        Ejecuta el procesamiento de todos los archivos
        
        Returns:
            Diccionario con {path_archivo: éxito}
        """
        # Encontrar archivos raw
        raw_files = self.find_raw_files()
        
        if not raw_files:
            logger.warning("No se encontraron archivos para procesar")
            return {}
        
        logger.info(f"Encontrados {len(raw_files)} archivos para procesar")
        
        results = {}
        
        # Procesar archivos en paralelo
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_file = {executor.submit(self.process_file, file): file for file in raw_files}
            
            with tqdm(
                total=len(raw_files),
                desc="Procesando archivos",
                unit="archivos",
                disable=not sys.stdout.isatty()
            ) as pbar:
                for future in concurrent.futures.as_completed(future_to_file):
                    try:
                        output_path, success = future.result()
                        file = future_to_file[future]
                        results[file] = success
                    except Exception as e:
                        file = future_to_file[future]
                        logger.error(f"Error al procesar {file}: {e}")
                        results[file] = False
                    
                    pbar.update(1)
        
        # Resultados
        successful = sum(1 for success in results.values() if success)
        logger.info(f"Procesamiento Bronze completado: {successful}/{len(results)} archivos exitosos")
        
        return results
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """
        Obtiene estadísticas del procesamiento
        
        Returns:
            Diccionario con estadísticas
        """
        stats = {
            'total_files': 0,
            'total_size_bytes': 0,
            'by_date': {}
        }
        
        for root, _, files in os.walk(self.output_dir):
            for file in files:
                if file.endswith('.parquet'):
                    file_path = Path(root) / file
                    stats['total_files'] += 1
                    file_size = file_path.stat().st_size
                    stats['total_size_bytes'] += file_size
                    
                    # Intentar extraer fecha de la estructura de carpetas
                    path_parts = file_path.parts
                    date_parts = [p for p in path_parts if len(p) == 4 and p.isdigit()]
                    
                    if date_parts:
                        year = date_parts[0]
                        
                        if year not in stats['by_date']:
                            stats['by_date'][year] = {
                                'count': 0,
                                'size_bytes': 0
                            }
                        
                        stats['by_date'][year]['count'] += 1
                        stats['by_date'][year]['size_bytes'] += file_size
        
        # Convertir tamaños a MB para más legibilidad
        stats['total_size_mb'] = stats['total_size_bytes'] / (1024 * 1024)
        for year in stats['by_date']:
            stats['by_date'][year]['size_mb'] = stats['by_date'][year]['size_bytes'] / (1024 * 1024)
        
        return stats


def parse_arguments():
    """
    Parsea los argumentos de línea de comandos
    
    Returns:
        Argumentos parseados
    """
    parser = argparse.ArgumentParser(description="Procesador de datos Bronze para GitHub Archive")
    
    parser.add_argument(
        "--input-dir",
        type=str,
        default=str(RAW_DATA_DIR),
        help=f"Directorio con archivos raw (default: {RAW_DATA_DIR})"
    )
    
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(BRONZE_DATA_DIR),
        help=f"Directorio para guardar archivos bronze (default: {BRONZE_DATA_DIR})"
    )
    
    parser.add_argument(
        "--max-workers",
        type=int,
        default=5,
        help="Número máximo de trabajadores para procesamiento paralelo (default: 5)"
    )
    
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100000,
        help="Número de registros a procesar por lote (default: 100000)"
    )
    
    parser.add_argument(
        "--flat",
        action="store_true",
        help="Guardar todos los archivos en la raíz sin preservar estructura"
    )
    
    parser.add_argument(
        "--compression",
        type=str,
        choices=['snappy', 'gzip', 'brotli', 'none'],
        default='snappy',
        help="Método de compresión para archivos parquet (default: snappy)"
    )
    
    return parser.parse_args()


def main():
    """Función principal"""
    try:
        # Parsear argumentos
        args = parse_arguments()
        
        # Ajustar compresión si es 'none'
        compression = None if args.compression == 'none' else args.compression
        
        # Inicializar procesador
        processor = BronzeProcessor(
            input_dir=Path(args.input_dir),
            output_dir=Path(args.output_dir),
            max_workers=args.max_workers,
            batch_size=args.batch_size,
            preserve_path_structure=not args.flat,
            compression=compression
        )
        
        # Ejecutar procesamiento
        results = processor.run()
        
        if not results:
            print("\n⚠️ No se encontraron archivos para procesar")
            return 0
        
        # Obtener estadísticas
        stats = processor.get_processing_stats()
        
        # Informar resultados
        successful = sum(1 for success in results.values() if success)
        total = len(results)
        
        if successful == total:
            print(f"\n✅ Procesamiento Bronze completado exitosamente: {successful}/{total} archivos")
            print(f"📊 Total procesado: {stats.get('total_size_mb', 0):.2f} MB en {stats.get('total_files', 0)} archivos")
            print(f"📂 Archivos guardados en: {args.output_dir}")
            return 0
        else:
            print(f"\n⚠️ Procesamiento Bronze completado con advertencias: {successful}/{total} archivos")
            print(f"❌ {total - successful} archivos fallaron, ver logs para detalles")
            print(f"📊 Total procesado: {stats.get('total_size_mb', 0):.2f} MB en {stats.get('total_files', 0)} archivos")
            return 1
    
    except KeyboardInterrupt:
        logger.info("Procesamiento interrumpido por el usuario")
        print("\n⚠️ Procesamiento interrumpido por el usuario")
        return 130
    
    except Exception as e:
        logger.critical(f"Error en el procesamiento: {e}", exc_info=True)
        print(f"\n❌ Error en el procesamiento: {e}")
        return 2


if __name__ == "__main__":
    sys.exit(main())