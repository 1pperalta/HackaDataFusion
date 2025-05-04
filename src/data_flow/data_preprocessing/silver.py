#!/usr/bin/env python
"""
GitHub Archive Silver Processor

Este script procesa los datos de la capa Bronze de GitHub Archive 
y los transforma para la capa Silver, aplicando limpieza,
normalización y extracción de entidades principales.

La capa Silver contiene datos limpios, normalizados y enriquecidos,
organizados en tablas dimensionales para facilitar análisis.
"""

import os
import sys
import logging
import argparse
import datetime
import json
import glob
from pathlib import Path
from typing import List, Dict, Any, Optional, Generator, Tuple
import time
import hashlib
import re

import pandas as pd
from tqdm import tqdm

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/silver_processor.log", mode='a')
    ]
)
logger = logging.getLogger("silver_processor")

# Directorios para datos
BRONZE_DIR = Path("data/processed/bronze")
SILVER_DIR = Path("data/processed/silver")

# Definir las tablas de la capa Silver
SILVER_TABLES = {
    "events": [
        "event_id", "event_hash", "event_type", "created_at", 
        "actor_id", "repo_id", "org_id", "is_bot", "public",
        "hour_bucket", "processed_at"
    ],
    "actors": [
        "actor_id", "actor_login", "actor_display_login", 
        "actor_url", "actor_type", "actor_site_admin", 
        "avatar_url", "gravatar_id", "first_seen_at", "last_seen_at"
    ],
    "repositories": [
        "repo_id", "repo_name", "repo_url", "owner_id",
        "owner_login", "is_fork", "language", "created_at", 
        "updated_at", "first_seen_at", "last_seen_at", "stars_count",
        "forks_count", "issues_count", "watchers_count", "size"
    ],
    "organizations": [
        "org_id", "org_login", "org_url", "avatar_url",
        "description", "first_seen_at", "last_seen_at"
    ],
    "payload_details": [
        "event_id", "event_type", "payload_action", 
        "payload_issue_id", "payload_pull_request_id",
        "payload_comment_id", "payload_push_size", 
        "payload_ref", "payload_ref_type", "payload_head", 
        "payload_before", "payload_size", "payload_distinct_size"
    ]
}

# Expresión regular para detectar bots
BOT_PATTERN = re.compile(r'(-bot|[._]bot|bot[._]|^bot-|^bot$|\[bot\]$)', re.IGNORECASE)


class SilverProcessor:
    """Procesador de capa Silver para eventos de GitHub"""
    
    def __init__(
        self,
        bronze_dir: Path = BRONZE_DIR,
        silver_dir: Path = SILVER_DIR,
        batch_size: int = 10000,
        use_csv: bool = True
    ):
        """
        Inicializa el procesador de capa Silver
        
        Args:
            bronze_dir: Directorio con datos de capa Bronze
            silver_dir: Directorio para datos de capa Silver
            batch_size: Tamaño de lote para procesar eventos
            use_csv: Si se debe usar CSV en lugar de parquet
        """
        self.bronze_dir = bronze_dir
        self.silver_dir = silver_dir
        self.batch_size = batch_size
        self.use_csv = use_csv
        
        # Crear directorios si no existen
        self.silver_dir.mkdir(parents=True, exist_ok=True)
        for table in SILVER_TABLES:
            (self.silver_dir / table).mkdir(parents=True, exist_ok=True)
        
        # Para mantener entidades en memoria durante el procesamiento de lotes
        self.actors_cache = {}
        self.repos_cache = {}
        self.orgs_cache = {}
    
    def find_bronze_files(self, date_pattern: Optional[str] = None) -> List[Path]:
        """
        Encuentra archivos en el directorio de datos de Bronze
        
        Args:
            date_pattern: Patrón de fecha opcional (YYYY/MM/DD)
            
        Returns:
            Lista de rutas a archivos .csv o .parquet
        """
        files = []
        
        if date_pattern:
            search_pattern_csv = f"{self.bronze_dir}/{date_pattern}/**/*.csv"
            search_pattern_parquet = f"{self.bronze_dir}/{date_pattern}/**/*.parquet"
        else:
            search_pattern_csv = f"{self.bronze_dir}/**/*.csv"
            search_pattern_parquet = f"{self.bronze_dir}/**/*.parquet"
        
        # Buscar archivos CSV
        csv_files = glob.glob(search_pattern_csv, recursive=True)
        files.extend([Path(file) for file in csv_files])
        
        # Buscar archivos Parquet
        parquet_files = glob.glob(search_pattern_parquet, recursive=True)
        files.extend([Path(file) for file in parquet_files])
        
        return files
    
    def read_bronze_file(self, file_path: Path) -> pd.DataFrame:
        """
        Lee un archivo de la capa Bronze
        
        Args:
            file_path: Ruta al archivo
            
        Returns:
            DataFrame con datos de Bronze
        """
        try:
            if file_path.suffix == '.csv':
                df = pd.read_csv(file_path)
            elif file_path.suffix == '.parquet':
                df = pd.read_parquet(file_path)
            else:
                raise ValueError(f"Formato de archivo no soportado: {file_path.suffix}")
            
            return df
        except Exception as e:
            logger.error(f"Error al leer archivo {file_path}: {e}")
            raise
    
    def parse_raw_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parsea el campo raw_data que contiene el JSON completo
        
        Args:
            df: DataFrame con datos de Bronze
            
        Returns:
            DataFrame con datos parseados
        """
        # Crear columna para raw_data parseado
        parsed_data = []
        
        for _, row in df.iterrows():
            try:
                if 'raw_data' in row:
                    raw_data = json.loads(row['raw_data'])
                    parsed_data.append(raw_data)
                else:
                    # Si no hay raw_data, usar un diccionario vacío
                    parsed_data.append({})
            except json.JSONDecodeError:
                # En caso de error, usar un diccionario vacío
                parsed_data.append({})
        
        return pd.DataFrame(parsed_data)
    
    def extract_actor_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extrae datos de los actores
        
        Args:
            df: DataFrame con datos parseados
            
        Returns:
            DataFrame con datos de actores
        """
        actors = []
        current_time = datetime.datetime.now().isoformat()
        
        for _, row in df.iterrows():
            if 'actor' in row and isinstance(row['actor'], dict):
                actor = row['actor'].copy()
                
                # Añadir datos de seguimiento temporal
                actor_id = actor.get('id')
                if actor_id:
                    if actor_id in self.actors_cache:
                        # Actualizar last_seen_at
                        actor['first_seen_at'] = self.actors_cache[actor_id]['first_seen_at']
                        actor['last_seen_at'] = current_time
                    else:
                        # Nuevo actor
                        actor['first_seen_at'] = current_time
                        actor['last_seen_at'] = current_time
                    
                    # Renombrar campos para mantener consistencia
                    if 'login' in actor:
                        actor['actor_login'] = actor.pop('login')
                    if 'display_login' in actor:
                        actor['actor_display_login'] = actor.pop('display_login')
                    if 'url' in actor:
                        actor['actor_url'] = actor.pop('url')
                    if 'site_admin' in actor:
                        actor['actor_site_admin'] = actor.pop('site_admin')
                    
                    # Detectar si es un bot
                    login = actor.get('actor_login', '')
                    actor['is_bot'] = bool(BOT_PATTERN.search(login)) if login else False
                    
                    # Añadir actor al cache
                    self.actors_cache[actor_id] = actor
                    
                    # Añadir a la lista
                    actors.append(actor)
        
        return pd.DataFrame(actors) if actors else pd.DataFrame()
    
    def extract_repo_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extrae datos de los repositorios
        
        Args:
            df: DataFrame con datos parseados
            
        Returns:
            DataFrame con datos de repositorios
        """
        repos = []
        current_time = datetime.datetime.now().isoformat()
        
        for _, row in df.iterrows():
            if 'repo' in row and isinstance(row['repo'], dict):
                repo = row['repo'].copy()
                
                # Añadir datos de seguimiento temporal
                repo_id = repo.get('id')
                if repo_id:
                    if repo_id in self.repos_cache:
                        # Actualizar last_seen_at
                        repo['first_seen_at'] = self.repos_cache[repo_id]['first_seen_at']
                        repo['last_seen_at'] = current_time
                    else:
                        # Nuevo repo
                        repo['first_seen_at'] = current_time
                        repo['last_seen_at'] = current_time
                    
                    # Renombrar campos para mantener consistencia
                    if 'name' in repo:
                        repo['repo_name'] = repo.pop('name')
                    if 'url' in repo:
                        repo['repo_url'] = repo.pop('url')
                    
                    # Extraer owner del nombre del repo
                    if 'repo_name' in repo:
                        parts = repo['repo_name'].split('/')
                        if len(parts) >= 2:
                            repo['owner_login'] = parts[0]
                    
                    # Añadir repo al cache
                    self.repos_cache[repo_id] = repo
                    
                    # Añadir a la lista
                    repos.append(repo)
        
        return pd.DataFrame(repos) if repos else pd.DataFrame()
    
    def extract_org_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extrae datos de las organizaciones
        
        Args:
            df: DataFrame con datos parseados
            
        Returns:
            DataFrame con datos de organizaciones
        """
        orgs = []
        current_time = datetime.datetime.now().isoformat()
        
        for _, row in df.iterrows():
            if 'org' in row and isinstance(row['org'], dict):
                org = row['org'].copy()
                
                # Añadir datos de seguimiento temporal
                org_id = org.get('id')
                if org_id:
                    if org_id in self.orgs_cache:
                        # Actualizar last_seen_at
                        org['first_seen_at'] = self.orgs_cache[org_id]['first_seen_at']
                        org['last_seen_at'] = current_time
                    else:
                        # Nueva organización
                        org['first_seen_at'] = current_time
                        org['last_seen_at'] = current_time
                    
                    # Renombrar campos para mantener consistencia
                    if 'login' in org:
                        org['org_login'] = org.pop('login')
                    if 'url' in org:
                        org['org_url'] = org.pop('url')
                    
                    # Añadir org al cache
                    self.orgs_cache[org_id] = org
                    
                    # Añadir a la lista
                    orgs.append(org)
        
        return pd.DataFrame(orgs) if orgs else pd.DataFrame()
    
    def extract_event_data(self, df: pd.DataFrame, file_date_str: str) -> pd.DataFrame:
        """
        Extrae datos principales de los eventos
        
        Args:
            df: DataFrame con datos parseados
            file_date_str: Fecha del archivo
            
        Returns:
            DataFrame con datos de eventos
        """
        events = []
        current_time = datetime.datetime.now().isoformat()
        
        for _, row in df.iterrows():
            event = {}
            
            # Campos principales
            event['event_id'] = row.get('id')
            event['event_hash'] = hashlib.md5(row.to_json().encode()).hexdigest()
            event['event_type'] = row.get('type')
            event['created_at'] = row.get('created_at')
            event['public'] = row.get('public')
            event['hour_bucket'] = file_date_str
            event['processed_at'] = current_time
            
            # Referencias a otras entidades
            if 'actor' in row and isinstance(row['actor'], dict):
                event['actor_id'] = row['actor'].get('id')
                
                # Detectar bot
                login = row['actor'].get('login', '')
                event['is_bot'] = bool(BOT_PATTERN.search(login)) if login else False
            
            if 'repo' in row and isinstance(row['repo'], dict):
                event['repo_id'] = row['repo'].get('id')
            
            if 'org' in row and isinstance(row['org'], dict):
                event['org_id'] = row['org'].get('id')
            
            events.append(event)
        
        return pd.DataFrame(events)
    
    def extract_payload_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extrae datos del payload
        
        Args:
            df: DataFrame con datos parseados
            
        Returns:
            DataFrame con datos de payload
        """
        payloads = []
        
        for _, row in df.iterrows():
            if 'payload' in row and isinstance(row['payload'], dict) and 'id' in row:
                payload = {
                    'event_id': row['id'],
                    'event_type': row.get('type')
                }
                
                # Extraer campos comunes
                payload['payload_action'] = row['payload'].get('action')
                
                # Extraer campos específicos según el tipo de evento
                if row.get('type') == 'IssuesEvent':
                    if 'issue' in row['payload'] and isinstance(row['payload']['issue'], dict):
                        payload['payload_issue_id'] = row['payload']['issue'].get('id')
                
                elif row.get('type') == 'PullRequestEvent':
                    if 'pull_request' in row['payload'] and isinstance(row['payload']['pull_request'], dict):
                        payload['payload_pull_request_id'] = row['payload']['pull_request'].get('id')
                
                elif row.get('type') == 'IssueCommentEvent' or row.get('type') == 'CommitCommentEvent':
                    if 'comment' in row['payload'] and isinstance(row['payload']['comment'], dict):
                        payload['payload_comment_id'] = row['payload']['comment'].get('id')
                
                elif row.get('type') == 'PushEvent':
                    payload['payload_push_size'] = row['payload'].get('size')
                    payload['payload_ref'] = row['payload'].get('ref')
                    payload['payload_head'] = row['payload'].get('head')
                    payload['payload_before'] = row['payload'].get('before')
                    payload['payload_size'] = row['payload'].get('size')
                    payload['payload_distinct_size'] = row['payload'].get('distinct_size')
                
                elif row.get('type') == 'CreateEvent' or row.get('type') == 'DeleteEvent':
                    payload['payload_ref'] = row['payload'].get('ref')
                    payload['payload_ref_type'] = row['payload'].get('ref_type')
                
                payloads.append(payload)
        
        return pd.DataFrame(payloads) if payloads else pd.DataFrame()
    
    def process_file_to_silver(self, file_path: Path) -> Dict[str, pd.DataFrame]:
        """
        Procesa un archivo de Bronze y lo convierte a tablas Silver
        
        Args:
            file_path: Ruta al archivo
            
        Returns:
            Diccionario con DataFrames para cada tabla Silver
        """
        # Leer archivo Bronze
        bronze_df = self.read_bronze_file(file_path)
        
        # Extraer fecha del archivo
        file_name = file_path.name
        file_date_str = file_name.split(".")[0]  # '2025-05-01-15'
        
        # Si hay datos de raw_data, parsearlos
        if 'raw_data' in bronze_df.columns:
            # Parsear raw_data
            parsed_df = self.parse_raw_data(bronze_df)
        else:
            # Si no hay raw_data, usar el DataFrame directamente
            parsed_df = bronze_df
        
        # Extraer tablas dimensionales
        actors_df = self.extract_actor_data(parsed_df)
        repos_df = self.extract_repo_data(parsed_df)
        orgs_df = self.extract_org_data(parsed_df)
        
        # Extraer tabla de eventos (hechos)
        events_df = self.extract_event_data(parsed_df, file_date_str)
        
        # Extraer detalles de payload
        payload_df = self.extract_payload_data(parsed_df)
        
        return {
            "events": events_df,
            "actors": actors_df,
            "repositories": repos_df,
            "organizations": orgs_df,
            "payload_details": payload_df
        }
    
    def save_silver_data(self, table_data: Dict[str, pd.DataFrame], date_part: str) -> Dict[str, Path]:
        """
        Guarda datos de capa Silver en archivos
        
        Args:
            table_data: Diccionario con DataFrames para cada tabla
            date_part: Parte de fecha (YYYY-MM-DD-HH)
            
        Returns:
            Rutas a los archivos guardados
        """
        saved_paths = {}
        
        for table_name, df in table_data.items():
            if df.empty:
                logger.info(f"No hay datos para la tabla {table_name}, omitiendo")
                continue
            
            # Asegurar que solo existen las columnas definidas en el esquema
            expected_cols = set(SILVER_TABLES.get(table_name, []))
            if expected_cols:
                # Filtrar columnas existentes
                existing_cols = [col for col in expected_cols if col in df.columns]
                df = df[existing_cols]
            
            # Crear carpeta para la tabla
            table_dir = self.silver_dir / table_name
            table_dir.mkdir(parents=True, exist_ok=True)
            
            # Guardar archivo
            try:
                file_name = f"{date_part}.{table_name}"
                if not self.use_csv:
                    try:
                        output_path = table_dir / f"{file_name}.parquet"
                        df.to_parquet(output_path, index=False)
                        saved_paths[table_name] = output_path
                    except ImportError as e:
                        logger.warning(f"Error al guardar como parquet: {e}")
                        logger.warning("Cambiando a formato CSV")
                        self.use_csv = True
                
                if self.use_csv:
                    output_path = table_dir / f"{file_name}.csv"
                    df.to_csv(output_path, index=False)
                    saved_paths[table_name] = output_path
            
            except Exception as e:
                logger.error(f"Error al guardar tabla {table_name}: {e}")
        
        return saved_paths
    
    def process_file(self, file_path: Path) -> Dict[str, Any]:
        """
        Procesa un archivo completo de Bronze a Silver
        
        Args:
            file_path: Ruta al archivo Bronze
            
        Returns:
            Diccionario con estadísticas de procesamiento
        """
        try:
            logger.info(f"Procesando archivo Bronze: {file_path}")
            
            file_name = file_path.name
            date_str = file_name.split(".")[0]  # '2025-05-01-15'
            
            # Verificar si ya existen archivos procesados para todas las tablas
            all_tables_exist = True
            for table_name in SILVER_TABLES:
                table_dir = self.silver_dir / table_name
                parquet_path = table_dir / f"{date_str}.{table_name}.parquet"
                csv_path = table_dir / f"{date_str}.{table_name}.csv"
                
                if not (parquet_path.exists() or csv_path.exists()):
                    all_tables_exist = False
                    break
            
            if all_tables_exist:
                logger.info(f"Archivos Silver ya existen para {date_str}, omitiendo")
                return {
                    "file": str(file_path),
                    "status": "skipped",
                    "tables": {},
                    "error": None
                }
            
            # Procesar archivo
            table_data = self.process_file_to_silver(file_path)
            
            # Guardar tablas
            saved_paths = self.save_silver_data(table_data, date_str)
            
            # Estadísticas por tabla
            table_stats = {}
            for table_name, df in table_data.items():
                table_stats[table_name] = {
                    "rows": len(df),
                    "path": str(saved_paths.get(table_name, None))
                }
            
            logger.info(f"Archivo {file_path} procesado exitosamente")
            return {
                "file": str(file_path),
                "status": "success",
                "tables": table_stats,
                "error": None
            }
            
        except Exception as e:
            logger.error(f"Error procesando archivo {file_path}: {e}", exc_info=True)
            return {
                "file": str(file_path),
                "status": "error",
                "tables": {},
                "error": str(e)
            }
    
    def run(self, date_pattern: Optional[str] = None) -> Dict[str, Any]:
        """
        Ejecuta el procesamiento de todos los archivos Bronze
        
        Args:
            date_pattern: Patrón de fecha opcional (YYYY/MM/DD)
            
        Returns:
            Estadísticas del procesamiento
        """
        # Encontrar archivos Bronze
        files = self.find_bronze_files(date_pattern)
        
        if not files:
            logger.warning(f"No se encontraron archivos Bronze para procesar")
            return {
                "processed_files": 0,
                "successful_files": 0,
                "skipped_files": 0,
                "failed_files": 0,
                "table_stats": {table: {"rows": 0} for table in SILVER_TABLES},
                "files": []
            }
        
        logger.info(f"Se encontraron {len(files)} archivos Bronze para procesar")
        
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
        
        # Contar filas por tabla
        table_stats = {table: {"rows": 0} for table in SILVER_TABLES}
        for result in successful:
            for table_name, stats in result["tables"].items():
                if table_name in table_stats:
                    table_stats[table_name]["rows"] += stats["rows"]
        
        stats = {
            "processed_files": len(files),
            "successful_files": len(successful),
            "skipped_files": len(skipped),
            "failed_files": len(failed),
            "table_stats": table_stats,
            "files": results
        }
        
        logger.info(f"Procesamiento Silver completado: {len(successful)}/{len(files)} archivos exitosos")
        
        return stats


def parse_arguments():
    """
    Parsea los argumentos de línea de comandos
    
    Returns:
        Argumentos parseados
    """
    parser = argparse.ArgumentParser(description="Procesador de capa Silver para datos de GitHub Archive")
    
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
    
    return parser.parse_args()


def main():
    """Función principal"""
    try:
        # Crear directorio de logs si no existe
        Path("logs").mkdir(exist_ok=True)
        
        # Parsear argumentos
        args = parse_arguments()
        
        # Inicializar procesador
        processor = SilverProcessor(
            batch_size=args.batch_size,
            use_csv=args.use_csv
        )
        
        # Ejecutar procesamiento
        stats = processor.run(date_pattern=args.date)
        
        # Mostrar resultados
        print(f"\n✅ Procesamiento Silver completado:")
        print(f"📊 {stats['successful_files']}/{stats['processed_files']} archivos procesados correctamente")
        print(f"⚠️ {stats['failed_files']} archivos fallidos")
        print(f"📝 {stats['skipped_files']} archivos omitidos (ya procesados)")
        
        # Mostrar estadísticas por tabla
        print("\nEstadísticas por tabla:")
        for table_name, table_stats in stats["table_stats"].items():
            print(f"  • {table_name}: {table_stats['rows']} filas")
        
        return 0
        
    except Exception as e:
        logger.critical(f"Error en el procesamiento Silver: {e}", exc_info=True)
        print(f"\n❌ Error en el procesamiento Silver: {e}")
        return 2


if __name__ == "__main__":
    sys.exit(main())