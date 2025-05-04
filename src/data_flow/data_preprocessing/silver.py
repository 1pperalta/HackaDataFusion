#!/usr/bin/env python
"""
GitHub Archive Silver Processing

Este script procesa los datos de la capa Bronze y los transforma en datos
más limpios y estructurados de la capa Silver, realizando:
- Normalización avanzada de datos
- Extracción de entidades (usuarios, repositorios)
- División en tablas específicas por tipo de evento
- Enriquecimiento con datos computados
- Transformaciones orientadas a análisis

Características:
- Procesamiento incremental
- Mantenimiento de esquemas consistentes
- Optimización de tipos de datos y columnas
- Particionado eficiente para consultas analíticas
"""

import os
import sys
import argparse
import logging
import datetime
import concurrent.futures
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional, Set
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
        logging.FileHandler("logs/silver_processing.log", mode='a')
    ]
)
logger = logging.getLogger("silver_processor")

# Directorios para bronze data y silver data
BRONZE_DATA_DIR = Path("data/processed/bronze")
SILVER_DATA_DIR = Path("data/processed/silver")

# Crear directorios si no existen
Path("logs").mkdir(exist_ok=True)
SILVER_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Definir subdirectorios para diferentes tipos de entidades
EVENTS_DIR = SILVER_DATA_DIR / "events"
ACTORS_DIR = SILVER_DATA_DIR / "actors"
REPOS_DIR = SILVER_DATA_DIR / "repos"
EVENT_TYPES_DIR = SILVER_DATA_DIR / "event_types"

# Crear subdirectorios
for dir_path in [EVENTS_DIR, ACTORS_DIR, REPOS_DIR, EVENT_TYPES_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# Subdirectorios para tipos específicos de eventos
PR_EVENTS_DIR = EVENT_TYPES_DIR / "pull_requests"
PUSH_EVENTS_DIR = EVENT_TYPES_DIR / "pushes"
ISSUE_EVENTS_DIR = EVENT_TYPES_DIR / "issues"
COMMENT_EVENTS_DIR = EVENT_TYPES_DIR / "comments"
OTHER_EVENTS_DIR = EVENT_TYPES_DIR / "other"

for dir_path in [PR_EVENTS_DIR, PUSH_EVENTS_DIR, ISSUE_EVENTS_DIR, COMMENT_EVENTS_DIR, OTHER_EVENTS_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)


class SilverProcessor:
    """Procesador de datos Bronze a Silver para GitHub Archive"""
    
    def __init__(
        self,
        input_dir: Path = BRONZE_DATA_DIR,
        output_dir: Path = SILVER_DATA_DIR,
        max_workers: int = 4,
        compression: str = 'snappy',
        events_per_file: int = 500000
    ):
        """
        Inicializa el procesador de la capa Silver
        
        Args:
            input_dir: Directorio con archivos bronze
            output_dir: Directorio para guardar archivos silver
            max_workers: Número máximo de trabajadores para procesamiento paralelo
            compression: Método de compresión para archivos parquet
            events_per_file: Número máximo de eventos por archivo procesado
        """
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.max_workers = max_workers
        self.compression = compression
        self.events_per_file = events_per_file
        
        # Crear directorio de salida si no existe
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def find_bronze_files(self) -> List[Path]:
        """
        Encuentra todos los archivos .parquet en el directorio de entrada
        
        Returns:
            Lista de rutas a archivos
        """
        files = []
        
        for item in self.input_dir.glob('**/*.parquet'):
            if item.is_file():
                files.append(item)
        
        return files
    
    def get_output_paths(self, input_path: Path) -> Dict[str, Path]:
        """
        Genera las rutas de salida para las diferentes tablas derivadas de un archivo
        
        Args:
            input_path: Ruta del archivo de entrada
            
        Returns:
            Diccionario con rutas de salida por tipo
        """
        # Extraer componentes de fecha del nombre o ruta del archivo
        date_components = self._extract_date_components(input_path)
        
        if date_components:
            year, month, day, hour = date_components
            date_str = f"{year}-{month}-{day}"
            file_prefix = f"{date_str}-{hour}"
        else:
            # Si no se puede extraer fecha, usar el nombre del archivo
            file_prefix = input_path.stem
        
        # Construir rutas de salida
        output_paths = {
            'events': EVENTS_DIR / f"{file_prefix}_events.parquet",
            'actors': ACTORS_DIR / f"{file_prefix}_actors.parquet",
            'repos': REPOS_DIR / f"{file_prefix}_repos.parquet",
            'pull_request_events': PR_EVENTS_DIR / f"{file_prefix}_pr_events.parquet",
            'push_events': PUSH_EVENTS_DIR / f"{file_prefix}_push_events.parquet",
            'issue_events': ISSUE_EVENTS_DIR / f"{file_prefix}_issue_events.parquet",
            'comment_events': COMMENT_EVENTS_DIR / f"{file_prefix}_comment_events.parquet",
            'other_events': OTHER_EVENTS_DIR / f"{file_prefix}_other_events.parquet"
        }
        
        # Asegurarse de que los directorios de salida existan
        for out_path in output_paths.values():
            out_path.parent.mkdir(parents=True, exist_ok=True)
        
        return output_paths
    
    def _extract_date_components(self, file_path: Path) -> Optional[Tuple[str, str, str, str]]:
        """
        Extrae componentes de fecha/hora de la ruta del archivo
        
        Args:
            file_path: Ruta del archivo
            
        Returns:
            Tupla con (año, mes, día, hora) o None si no se encuentra
        """
        parts = file_path.parts
        
        # Buscar partes de fecha en el camino o nombre del archivo
        for part in parts + (file_path.stem,):
            if "-" in str(part):
                # Intentar formato YYYY-MM-DD-HH
                date_parts = str(part).split("-")
                if len(date_parts) >= 4 and len(date_parts[0]) == 4:
                    try:
                        year = date_parts[0]
                        month = date_parts[1].zfill(2)
                        day = date_parts[2].zfill(2)
                        hour = date_parts[3].zfill(2)
                        return (year, month, day, hour)
                    except (IndexError, ValueError):
                        pass
        
        # Buscar componentes en las carpetas (año/mes/día)
        year_parts = [p for p in parts if len(str(p)) == 4 and str(p).isdigit()]
        month_parts = [p for p in parts if len(str(p)) in (1, 2) and str(p).isdigit() and int(p) <= 12]
        day_parts = [p for p in parts if len(str(p)) in (1, 2) and str(p).isdigit() and int(p) <= 31]
        
        if year_parts and month_parts and day_parts:
            year = year_parts[0]
            month = str(month_parts[0]).zfill(2)
            day = str(day_parts[0]).zfill(2)
            
            # Intentar encontrar hora en el nombre del archivo
            file_name = file_path.stem
            for part in file_name.split("-"):
                if part.isdigit() and int(part) < 24:
                    hour = part.zfill(2)
                    return (year, month, day, hour)
            
            # Si no hay hora en el nombre, usar '00'
            return (year, month, day, "00")
        
        return None
    
    def process_file(self, input_path: Path) -> Tuple[Dict[str, Path], bool]:
        """
        Procesa un archivo bronze y lo convierte en diferentes tablas silver
        
        Args:
            input_path: Ruta del archivo de entrada
            
        Returns:
            Tupla con (diccionario de rutas_salida, éxito)
        """
        output_paths = self.get_output_paths(input_path)
        
        # Verificar si todos los archivos de salida ya existen
        all_exist = all(path.exists() for path in output_paths.values())
        if all_exist:
            logger.info(f"Todos los archivos de salida para {input_path.name} ya existen, omitiendo")
            return output_paths, True
            
        try:
            logger.info(f"Procesando archivo {input_path}")
            
            # Leer archivo bronze
            df = pd.read_parquet(input_path)
            
            if df.empty:
                logger.warning(f"El archivo {input_path} no contiene datos")
                return output_paths, False
            
            # Procesar datos en las diferentes tablas
            success = self._process_tables(df, output_paths)
            
            if success:
                logger.info(f"Archivo {input_path.name} procesado exitosamente")
            else:
                logger.error(f"Errores al procesar las tablas de {input_path.name}")
                
            return output_paths, success
            
        except Exception as e:
            logger.error(f"Error al procesar {input_path}: {e}", exc_info=True)
            # Eliminar archivos parcialmente creados
            for path in output_paths.values():
                if path.exists():
                    path.unlink()
            return output_paths, False
    
    def _process_tables(self, df: pd.DataFrame, output_paths: Dict[str, Path]) -> bool:
        """
        Procesa un DataFrame en múltiples tablas
        
        Args:
            df: DataFrame con datos bronze
            output_paths: Diccionario con rutas de salida
            
        Returns:
            True si todas las tablas se procesaron correctamente
        """
        try:
            # 1. Procesar tabla de eventos base (columnas generales)
            events_df = self._extract_events_table(df)
            
            # 2. Extraer entidades: actores y repositorios
            actors_df = self._extract_actors_table(df)
            repos_df = self._extract_repos_table(df)
            
            # 3. Extraer tablas específicas por tipo de evento
            pr_events_df = self._extract_pr_events(df)
            push_events_df = self._extract_push_events(df)
            issue_events_df = self._extract_issue_events(df)
            comment_events_df = self._extract_comment_events(df)
            other_events_df = self._extract_other_events(df)
            
            # 4. Guardar todas las tablas
            tables = {
                'events': events_df,
                'actors': actors_df,
                'repos': repos_df,
                'pull_request_events': pr_events_df,
                'push_events': push_events_df,
                'issue_events': issue_events_df,
                'comment_events': comment_events_df,
                'other_events': other_events_df
            }
            
            for table_name, table_df in tables.items():
                if not table_df.empty:
                    table_df.to_parquet(
                        output_paths[table_name],
                        compression=self.compression,
                        index=False
                    )
                    logger.info(f"Tabla {table_name} guardada con {len(table_df)} filas")
            
            return True
            
        except Exception as e:
            logger.error(f"Error al procesar tablas: {e}", exc_info=True)
            return False
    
    def _extract_events_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extrae y normaliza la tabla base de eventos
        
        Args:
            df: DataFrame con datos bronze
            
        Returns:
            DataFrame de eventos normalizado
        """
        # Columnas básicas para todos los eventos
        base_columns = [
            'id', 'type', 'created_at', 'actor', 'repo', 'public'
        ]
        
        # Columnas comunes
        common_columns = base_columns + [
            'event_date', 'event_hour', 
        ]
        
        # Extraer campos básicos
        events_df = pd.DataFrame()
        
        # Verificar qué columnas existen
        available_columns = [col for col in common_columns if col in df.columns]
        
        if available_columns:
            events_df = df[available_columns].copy()
        else:
            # Si no hay columnas básicas, retornar un DataFrame vacío
            logger.warning("No se encontraron columnas básicas de eventos")
            return pd.DataFrame()
        
        # Normalizar dataframe
        events_df = self._normalize_events(events_df)
        
        # Extraer IDs de entidades relacionadas
        if 'actor' in events_df.columns and isinstance(events_df['actor'].iloc[0] if len(events_df) > 0 else None, dict):
            events_df['actor_id'] = events_df['actor'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)
            events_df['actor_login'] = events_df['actor'].apply(lambda x: x.get('login') if isinstance(x, dict) else None)
        
        if 'repo' in events_df.columns and isinstance(events_df['repo'].iloc[0] if len(events_df) > 0 else None, dict):
            events_df['repo_id'] = events_df['repo'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)
            events_df['repo_name'] = events_df['repo'].apply(lambda x: x.get('name') if isinstance(x, dict) else None)
        
        # Eliminar columnas nested originales una vez extraídas
        if 'actor_id' in events_df.columns and 'actor' in events_df.columns:
            events_df = events_df.drop('actor', axis=1)
        
        if 'repo_id' in events_df.columns and 'repo' in events_df.columns:
            events_df = events_df.drop('repo', axis=1)
        
        # Asegurar que id es columna primaria
        if 'id' in events_df.columns:
            events_df = events_df.drop_duplicates('id')
            events_df = events_df.rename(columns={'id': 'event_id'})
        
        return events_df
    
    def _normalize_events(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza el DataFrame de eventos
        
        Args:
            df: DataFrame a normalizar
            
        Returns:
            DataFrame normalizado
        """
        # Asegurar que created_at es datetime
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
            
            # Si no existen event_date/event_hour, crearlos desde created_at
            if 'event_date' not in df.columns and 'created_at' in df.columns:
                df['event_date'] = df['created_at'].dt.strftime('%Y-%m-%d')
            
            if 'event_hour' not in df.columns and 'created_at' in df.columns:
                df['event_hour'] = df['created_at'].dt.strftime('%H')
        
        # Convertir id a string si es numérico (para consistencia)
        if 'id' in df.columns and pd.api.types.is_numeric_dtype(df['id']):
            df['id'] = df['id'].astype(str)
        
        return df
    
    def _extract_actors_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extrae y normaliza tabla de actores (usuarios)
        
        Args:
            df: DataFrame con datos bronze
            
        Returns:
            DataFrame de actores normalizado
        """
        # Verificar si existe la columna actor
        if 'actor' not in df.columns:
            return pd.DataFrame()
        
        # Extraer datos de actores
        actors = []
        
        for _, row in df.iterrows():
            if isinstance(row.get('actor'), dict):
                actors.append(row['actor'])
        
        if not actors:
            return pd.DataFrame()
        
        # Convertir a DataFrame
        actors_df = pd.DataFrame(actors)
        
        # Normalizar columnas básicas
        basic_columns = ['id', 'login', 'display_login', 'gravatar_id', 'url', 'avatar_url']
        available_columns = [col for col in basic_columns if col in actors_df.columns]
        
        if not available_columns:
            return pd.DataFrame()
        
        actors_df = actors_df[available_columns].copy()
        actors_df = actors_df.rename(columns={'id': 'actor_id', 'login': 'actor_login'})
        actors_df = actors_df.drop_duplicates('actor_id')
        
        return actors_df
    
    def _extract_repos_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extrae y normaliza tabla de repositorios
        
        Args:
            df: DataFrame con datos bronze
            
        Returns:
            DataFrame de repositorios normalizado
        """
        # Verificar si existe la columna repo
        if 'repo' not in df.columns:
            return pd.DataFrame()
        
        # Extraer datos de repos
        repos = []
        
        for _, row in df.iterrows():
            if isinstance(row.get('repo'), dict):
                repos.append(row['repo'])
        
        if not repos:
            return pd.DataFrame()
        
        # Convertir a DataFrame
        repos_df = pd.DataFrame(repos)
        
        # Normalizar columnas básicas
        basic_columns = ['id', 'name', 'url']
        available_columns = [col for col in basic_columns if col in repos_df.columns]
        
        if not available_columns:
            return pd.DataFrame()
        
        repos_df = repos_df[available_columns].copy()
        repos_df = repos_df.rename(columns={'id': 'repo_id', 'name': 'repo_name'})
        repos_df = repos_df.drop_duplicates('repo_id')
        
        # Extraer owner/repo del nombre
        if 'repo_name' in repos_df.columns:
            repos_df['owner_name'] = repos_df['repo_name'].apply(
                lambda x: x.split('/')[0] if isinstance(x, str) and '/' in x else None
            )
            repos_df['repo_short_name'] = repos_df['repo_name'].apply(
                lambda x: x.split('/')[-1] if isinstance(x, str) and '/' in x else x
            )
        
        return repos_df
    
    def _extract_pr_events(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extrae y normaliza eventos de pull request
        
        Args:
            df: DataFrame con datos bronze
            
        Returns:
            DataFrame de eventos PR normalizado
        """
        # Filtrar sólo PR events
        pr_types = ['PullRequestEvent', 'PullRequestReviewEvent', 'PullRequestReviewCommentEvent']
        pr_df = df[df['type'].isin(pr_types)].copy() if 'type' in df.columns else pd.DataFrame()
        
        if pr_df.empty:
            return pd.DataFrame()
            
        # Extraer id de evento y referencias a actores/repos
        base_columns = ['id', 'created_at', 'type']
        pr_events = pd.DataFrame()
        
        # Ver qué columnas están disponibles
        available_columns = [col for col in base_columns if col in pr_df.columns]
        
        if available_columns:
            pr_events = pr_df[available_columns].copy()
            pr_events = pr_events.rename(columns={'id': 'event_id'})
            
            # Extraer IDs de entidades relacionadas
            if 'actor' in pr_df.columns:
                pr_events['actor_id'] = pr_df['actor'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)
            
            if 'repo' in pr_df.columns:
                pr_events['repo_id'] = pr_df['repo'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)
                pr_events['repo_name'] = pr_df['repo'].apply(lambda x: x.get('name') if isinstance(x, dict) else None)
            
            # Extraer detalles específicos del payload
            if 'payload' in pr_df.columns:
                pr_events['action'] = pr_df['payload'].apply(lambda x: x.get('action') if isinstance(x, dict) else None)
                pr_events['pr_number'] = pr_df['payload'].apply(
                    lambda x: x.get('number') or (x.get('pull_request', {}).get('number') if isinstance(x, dict) else None)
                )
                
                # Extraer estado de PR
                pr_events['pr_state'] = pr_df['payload'].apply(
                    lambda x: (x.get('pull_request', {}).get('state') if isinstance(x, dict) and isinstance(x.get('pull_request'), dict) else None)
                )
                
                # Extraer si fue merged
                pr_events['pr_merged'] = pr_df['payload'].apply(
                    lambda x: (x.get('pull_request', {}).get('merged') if isinstance(x, dict) and isinstance(x.get('pull_request'), dict) else None)
                )
                
                # Extraer número de commits, adiciones, eliminaciones
                pr_events['commits'] = pr_df['payload'].apply(
                    lambda x: (x.get('pull_request', {}).get('commits') if isinstance(x, dict) and isinstance(x.get('pull_request'), dict) else None)
                )
                
                pr_events['additions'] = pr_df['payload'].apply(
                    lambda x: (x.get('pull_request', {}).get('additions') if isinstance(x, dict) and isinstance(x.get('pull_request'), dict) else None)
                )
                
                pr_events['deletions'] = pr_df['payload'].apply(
                    lambda x: (x.get('pull_request', {}).get('deletions') if isinstance(x, dict) and isinstance(x.get('pull_request'), dict) else None)
                )
                
                # Extraer título del PR
                pr_events['title'] = pr_df['payload'].apply(
                    lambda x: (x.get('pull_request', {}).get('title') if isinstance(x, dict) and isinstance(x.get('pull_request'), dict) else None)
                )
        
        return pr_events
    
    def _extract_push_events(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extrae y normaliza eventos de push
        
        Args:
            df: DataFrame con datos bronze
            
        Returns:
            DataFrame de eventos Push normalizado
        """
        # Filtrar sólo Push events
        push_df = df[df['type'] == 'PushEvent'].copy() if 'type' in df.columns else pd.DataFrame()
        
        if push_df.empty:
            return pd.DataFrame()
            
        # Extraer columnas base
        base_columns = ['id', 'created_at', 'type']
        push_events = pd.DataFrame()
        
        # Ver qué columnas están disponibles
        available_columns = [col for col in base_columns if col in push_df.columns]
        
        if available_columns:
            push_events = push_df[available_columns].copy()
            push_events = push_events.rename(columns={'id': 'event_id'})
            
            # Extraer IDs de entidades relacionadas
            if 'actor' in push_df.columns:
                push_events['actor_id'] = push_df['actor'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)
            
            if 'repo' in push_df.columns:
                push_events['repo_id'] = push_df['repo'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)
                push_events['repo_name'] = push_df['repo'].apply(lambda x: x.get('name') if isinstance(x, dict) else None)
            
            # Extraer detalles del payload
            if 'payload' in push_df.columns:
                # Referencia (branch)
                push_events['ref'] = push_df['payload'].apply(lambda x: x.get('ref') if isinstance(x, dict) else None)
                
                # Extraer nombre de branch sin refs/heads/
                push_events['branch'] = push_events['ref'].apply(
                    lambda x: x.replace('refs/heads/', '') if isinstance(x, str) and x.startswith('refs/heads/') else x
                )
                
                # Número de commits
                push_events['commit_count'] = push_df['payload'].apply(lambda x: x.get('size') if isinstance(x, dict) else None)
                
                # SHA antes y después
                push_events['before'] = push_df['payload'].apply(lambda x: x.get('before') if isinstance(x, dict) else None)
                push_events['after'] = push_df['payload'].apply(lambda x: x.get('after') if isinstance(x, dict) else None)
                
                # Extraer información del primer commit si existe
                push_events['commit_sha'] = push_df['payload'].apply(
                    lambda x: x['commits'][0]['sha'] if isinstance(x, dict) and isinstance(x.get('commits'), list) and len(x.get('commits', [])) > 0 else None
                )
                
                push_events['commit_message'] = push_df['payload'].apply(
                    lambda x: x['commits'][0]['message'] if isinstance(x, dict) and isinstance(x.get('commits'), list) and len(x.get('commits', [])) > 0 else None
                )
                
                push_events['commit_author_name'] = push_df['payload'].apply(
                    lambda x: x['commits'][0]['author']['name'] if isinstance(x, dict) and isinstance(x.get('commits'), list) and len(x.get('commits', [])) > 0 else None
                )
                
                push_events['commit_author_email'] = push_df['payload'].apply(
                    lambda x: x['commits'][0]['author']['email'] if isinstance(x, dict) and isinstance(x.get('commits'), list) and len(x.get('commits', [])) > 0 else None
                )
        
        return push_events
    
    def _extract_issue_events(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extrae y normaliza eventos de issues
        
        Args:
            df: DataFrame con datos bronze
            
        Returns:
            DataFrame de eventos de issues normalizado
        """
        # Filtrar sólo Issue events
        issue_df = df[df['type'] == 'IssuesEvent'].copy() if 'type' in df.columns else pd.DataFrame()
        
        if issue_df.empty:
            return pd.DataFrame()
            
        # Extraer columnas base
        base_columns = ['id', 'created_at', 'type']
        issue_events = pd.DataFrame()
        
        # Ver qué columnas están disponibles
        available_columns = [col for col in base_columns if col in issue_df.columns]
        
        if available_columns:
            issue_events = issue_df[available_columns].copy()
            issue_events = issue_events.rename(columns={'id': 'event_id'})
            
            # Extraer IDs de entidades relacionadas
            if 'actor' in issue_df.columns:
                issue_events['actor_id'] = issue_df['actor'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)
            
            if 'repo' in issue_df.columns:
                issue_events['repo_id'] = issue_df['repo'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)
                issue_events['repo_name'] = issue_df['repo'].apply(lambda x: x.get('name') if isinstance(x, dict) else None)
            
            # Extraer detalles del payload
            if 'payload' in issue_df.columns:
                # Acción (opened, closed, etc)
                issue_events['action'] = issue_df['payload'].apply(lambda x: x.get('action') if isinstance(x, dict) else None)
                
                # Número de issue
                issue_events['issue_number'] = issue_df['payload'].apply(
                    lambda x: x.get('issue', {}).get('number') if isinstance(x, dict) and isinstance(x.get('issue'), dict) else None
                )
                
                # Estado del issue
                issue_events['issue_state'] = issue_df['payload'].apply(
                    lambda x: x.get('issue', {}).get('state') if isinstance(x, dict) and isinstance(x.get('issue'), dict) else None
                )
                
                # Título del issue
                issue_events['title'] = issue_df['payload'].apply(
                    lambda x: x.get('issue', {}).get('title') if isinstance(x, dict) and isinstance(x.get('issue'), dict) else None
                )
                
                # Labels
                issue_events['labels'] = issue_df['payload'].apply(
                    lambda x: [l.get('name') for l in x.get('issue', {}).get('labels', []) if isinstance(l, dict) and 'name' in l]
                    if isinstance(x, dict) and isinstance(x.get('issue'), dict) else None
                )
                
                # Verificar si es un issue o un PR (algunos repos manejan PRs como issues)
                issue_events['is_pull_request'] = issue_df['payload'].apply(
                    lambda x: 'pull_request' in x.get('issue', {})
                    if isinstance(x, dict) and isinstance(x.get('issue'), dict) else None
                )
        
        return issue_events
    
    def _extract_comment_events(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extrae y normaliza eventos de comentarios
        
        Args:
            df: DataFrame con datos bronze
            
        Returns:
            DataFrame de eventos de comentarios normalizado
        """
        # Filtrar sólo eventos de comentarios
        comment_types = ['IssueCommentEvent', 'CommitCommentEvent', 'PullRequestReviewCommentEvent']
        comment_df = df[df['type'].isin(comment_types)].copy() if 'type' in df.columns else pd.DataFrame()
        
        if comment_df.empty:
            return pd.DataFrame()
            
        # Extraer columnas base
        base_columns = ['id', 'created_at', 'type']
        comment_events = pd.DataFrame()
        
        # Ver qué columnas están disponibles
        available_columns = [col for col in base_columns if col in comment_df.columns]
        
        if available_columns:
            comment_events = comment_df[available_columns].copy()
            comment_events = comment_events.rename(columns={'id': 'event_id'})
            
            # Extraer IDs de entidades relacionadas
            if 'actor' in comment_df.columns:
                comment_events['actor_id'] = comment_df['actor'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)
            
            if 'repo' in comment_df.columns:
                comment_events['repo_id'] = comment_df['repo'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)
                comment_events['repo_name'] = comment_df['repo'].apply(lambda x: x.get('name') if isinstance(x, dict) else None)
            
            # Extraer detalles del payload
            if 'payload' in comment_df.columns:
                # Acción (created, edited, deleted)
                comment_events['action'] = comment_df['payload'].apply(lambda x: x.get('action') if isinstance(x, dict) else None)
                
                # ID del comentario
                comment_events['comment_id'] = comment_df['payload'].apply(
                    lambda x: x.get('comment', {}).get('id') if isinstance(x, dict) and isinstance(x.get('comment'), dict) else None
                )
                
                # Extraer a qué pertenece el comentario (issue, PR, commit)
                for field in ['issue', 'pull_request', 'commit']:
                    comment_events[f'{field}_id'] = comment_df['payload'].apply(
                        lambda x: (x.get(field, {}).get('id') or x.get('comment', {}).get(f'{field}_id'))
                        if isinstance(x, dict) and (isinstance(x.get(field), dict) or isinstance(x.get('comment'), dict)) else None
                    )
                
                # Número del issue o PR
                comment_events['issue_number'] = comment_df['payload'].apply(
                    lambda x: x.get('issue', {}).get('number')
                    if isinstance(x, dict) and isinstance(x.get('issue'), dict) else None
                )
                
                # Contenido del comentario
                comment_events['body_short'] = comment_df['payload'].apply(
                    lambda x: (x.get('comment', {}).get('body', '')[:100] if len(str(x.get('comment', {}).get('body', ''))) > 100 else x.get('comment', {}).get('body'))
                    if isinstance(x, dict) and isinstance(x.get('comment'), dict) else None
                )
        
        return comment_events
    
    def _extract_other_events(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extrae y normaliza otros tipos de eventos
        
        Args:
            df: DataFrame con datos bronze
            
        Returns:
            DataFrame de otros eventos normalizado
        """
        # Filtrar eventos que no son PR, Push, Issue o Comment
        event_types = [
            'PullRequestEvent', 'PullRequestReviewEvent', 'PullRequestReviewCommentEvent', 
            'PushEvent', 'IssuesEvent', 'IssueCommentEvent', 'CommitCommentEvent'
        ]
        
        other_df = df[~df['type'].isin(event_types)].copy() if 'type' in df.columns else pd.DataFrame()
        
        if other_df.empty:
            return pd.DataFrame()
            
        # Extraer columnas base
        base_columns = ['id', 'created_at', 'type']
        other_events = pd.DataFrame()
        
        # Ver qué columnas están disponibles
        available_columns = [col for col in base_columns if col in other_df.columns]
        
        if available_columns:
            other_events = other_df[available_columns].copy()
            other_events = other_events.rename(columns={'id': 'event_id'})
            
            # Extraer IDs de entidades relacionadas
            if 'actor' in other_df.columns:
                other_events['actor_id'] = other_df['actor'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)
            
            if 'repo' in other_df.columns:
                other_events['repo_id'] = other_df['repo'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)
                other_events['repo_name'] = other_df['repo'].apply(lambda x: x.get('name') if isinstance(x, dict) else None)
            
            # Extraer acción del payload si existe
            if 'payload' in other_df.columns:
                other_events['action'] = other_df['payload'].apply(lambda x: x.get('action') if isinstance(x, dict) else None)
        
        return other_events
    
    def run(self) -> Dict[Path, bool]:
        """
        Ejecuta el procesamiento de todos los archivos
        
        Returns:
            Diccionario con {path_archivo: éxito}
        """
        # Encontrar archivos bronze
        bronze_files = self.find_bronze_files()
        
        if not bronze_files:
            logger.warning("No se encontraron archivos Bronze para procesar")
            return {}
        
        logger.info(f"Encontrados {len(bronze_files)} archivos Bronze para procesar")
        
        # Resultados por archivo
        results = {}
        
        # Procesar archivos en paralelo
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_file = {executor.submit(self.process_file, file): file for file in bronze_files}
            
            with tqdm(
                total=len(bronze_files),
                desc="Procesando archivos Bronze",
                unit="archivos",
                disable=not sys.stdout.isatty()
            ) as pbar:
                for future in concurrent.futures.as_completed(future_to_file):
                    try:
                        output_paths, success = future.result()
                        file = future_to_file[future]
                        for output_path in output_paths.values():
                            results[output_path] = success
                    except Exception as e:
                        file = future_to_file[future]
                        logger.error(f"Error al procesar {file}: {e}")
                        results[file] = False
                    
                    pbar.update(1)
        
        # Resultados
        successful = sum(1 for success in results.values() if success)
        logger.info(f"Procesamiento Silver completado: {successful}/{len(results)} archivos exitosos")
        
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
            'by_type': {
                'events': {'count': 0, 'size_bytes': 0},
                'actors': {'count': 0, 'size_bytes': 0},
                'repos': {'count': 0, 'size_bytes': 0},
                'pull_requests': {'count': 0, 'size_bytes': 0},
                'pushes': {'count': 0, 'size_bytes': 0},
                'issues': {'count': 0, 'size_bytes': 0},
                'comments': {'count': 0, 'size_bytes': 0},
                'other': {'count': 0, 'size_bytes': 0}
            }
        }
        
        # Tipos de archivos y sus directorios
        type_dirs = {
            'events': EVENTS_DIR,
            'actors': ACTORS_DIR,
            'repos': REPOS_DIR,
            'pull_requests': PR_EVENTS_DIR,
            'pushes': PUSH_EVENTS_DIR,
            'issues': ISSUE_EVENTS_DIR,
            'comments': COMMENT_EVENTS_DIR,
            'other': OTHER_EVENTS_DIR
        }
        
        # Contar archivos y tamaños por tipo
        for type_name, dir_path in type_dirs.items():
            file_count = 0
            size_bytes = 0
            
            for file_path in dir_path.glob('*.parquet'):
                if file_path.is_file():
                    file_count += 1
                    size_bytes += file_path.stat().st_size
            
            stats['by_type'][type_name]['count'] = file_count
            stats['by_type'][type_name]['size_bytes'] = size_bytes
            stats['by_type'][type_name]['size_mb'] = size_bytes / (1024 * 1024)
            
            stats['total_files'] += file_count
            stats['total_size_bytes'] += size_bytes
        
        # Convertir tamaños a MB para más legibilidad
        stats['total_size_mb'] = stats['total_size_bytes'] / (1024 * 1024)
        
        return stats


def parse_arguments():
    """
    Parsea los argumentos de línea de comandos
    
    Returns:
        Argumentos parseados
    """
    parser = argparse.ArgumentParser(description="Procesador de datos Silver para GitHub Archive")
    
    parser.add_argument(
        "--input-dir",
        type=str,
        default=str(BRONZE_DATA_DIR),
        help=f"Directorio con archivos bronze (default: {BRONZE_DATA_DIR})"
    )
    
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(SILVER_DATA_DIR),
        help=f"Directorio para guardar archivos silver (default: {SILVER_DATA_DIR})"
    )
    
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Número máximo de trabajadores para procesamiento paralelo (default: 4)"
    )
    
    parser.add_argument(
        "--compression",
        type=str,
        choices=['snappy', 'gzip', 'brotli', 'none'],
        default='snappy',
        help="Método de compresión para archivos parquet (default: snappy)"
    )
    
    parser.add_argument(
        "--events-per-file",
        type=int,
        default=500000,
        help="Número máximo de eventos por archivo procesado (default: 500000)"
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
        processor = SilverProcessor(
            input_dir=Path(args.input_dir),
            output_dir=Path(args.output_dir),
            max_workers=args.max_workers,
            compression=compression,
            events_per_file=args.events_per_file
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
            print(f"\n✅ Procesamiento Silver completado exitosamente: {successful}/{total} archivos")
            print(f"📊 Total procesado: {stats.get('total_size_mb', 0):.2f} MB en {stats.get('total_files', 0)} archivos")
            print(f"📂 Archivos guardados en: {args.output_dir}")
            
            # Mostrar desglose por tipo
            for type_name, type_stats in stats.get('by_type', {}).items():
                if type_stats.get('count', 0) > 0:
                    print(f"  - {type_name}: {type_stats.get('count', 0)} archivos, {type_stats.get('size_mb', 0):.2f} MB")
            
            return 0
        else:
            print(f"\n⚠️ Procesamiento Silver completado con advertencias: {successful}/{total} archivos")
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