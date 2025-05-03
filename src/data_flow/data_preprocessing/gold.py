#!/usr/bin/env python
"""
GitHub Archive Gold Processing

Este script procesa los datos de la capa Silver y los transforma en datos
de la capa Gold, que incluye:
- Agregaciones avanzadas
- Métricas y KPIs pre-calculados
- Vistas dimensionales optimizadas para análisis
- Datos listos para consumo por herramientas de BI

Características:
- Generación de insights sobre actividad de repositorios
- Métricas de contribución de usuarios
- Análisis de tendencias temporales
- Estadísticas de desarrollo y colaboración
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
import json

import pandas as pd
import numpy as np
from tqdm import tqdm

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/gold_processing.log", mode='a')
    ]
)
logger = logging.getLogger("gold_processor")

# Directorios para silver data y gold data
SILVER_DATA_DIR = Path("data/silver")
GOLD_DATA_DIR = Path("data/gold")

# Crear directorios si no existen
Path("logs").mkdir(exist_ok=True)
GOLD_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Definir subdirectorios para diferentes tipos de vistas Gold
REPO_METRICS_DIR = GOLD_DATA_DIR / "repo_metrics"
USER_METRICS_DIR = GOLD_DATA_DIR / "user_metrics"
TIME_METRICS_DIR = GOLD_DATA_DIR / "time_metrics"
TOPN_DIR = GOLD_DATA_DIR / "top_n"

# Crear subdirectorios
for dir_path in [REPO_METRICS_DIR, USER_METRICS_DIR, TIME_METRICS_DIR, TOPN_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)


class GoldProcessor:
    """Procesador de datos Silver a Gold para GitHub Archive"""
    
    def __init__(
        self,
        input_dir: Path = SILVER_DATA_DIR,
        output_dir: Path = GOLD_DATA_DIR,
        max_workers: int = 3,
        date_range: Optional[Tuple[datetime.date, datetime.date]] = None
    ):
        """
        Inicializa el procesador de la capa Gold
        
        Args:
            input_dir: Directorio con archivos silver
            output_dir: Directorio para guardar archivos gold
            max_workers: Número máximo de trabajadores para procesamiento paralelo
            date_range: Rango de fechas opcional para limitar el procesamiento
        """
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.max_workers = max_workers
        self.date_range = date_range
        
        # Crear directorio de salida si no existe
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def find_silver_files(self, pattern: str = "**/*.parquet") -> Dict[str, List[Path]]:
        """
        Encuentra todos los archivos .parquet en el directorio de entrada,
        agrupados por tipo
        
        Args:
            pattern: Patrón glob para buscar archivos
            
        Returns:
            Diccionario con listas de rutas por tipo de archivo
        """
        files = {
            'events': [],
            'actors': [],
            'repos': [],
            'push_events': [],
            'pull_request_events': [],
            'issue_events': [],
            'comment_events': [],
            'other_events': []
        }
        
        # Buscar archivos por tipo
        for item in self.input_dir.glob(pattern):
            if not item.is_file():
                continue
                
            file_name = item.name.lower()
            
            # Clasificar por tipo
            for file_type in files.keys():
                if file_type in file_name:
                    # Si hay filtro de fecha, verificar si aplica
                    if self.date_range:
                        date_parts = self._extract_date_from_path(item)
                        if date_parts:
                            file_date = datetime.date(int(date_parts[0]), int(date_parts[1]), int(date_parts[2]))
                            if not (self.date_range[0] <= file_date <= self.date_range[1]):
                                continue
                    
                    files[file_type].append(item)
                    break
        
        return files
    
    def _extract_date_from_path(self, file_path: Path) -> Optional[List[str]]:
        """
        Extrae componentes de fecha de una ruta de archivo
        
        Args:
            file_path: Ruta del archivo
            
        Returns:
            Lista con [año, mes, día] o None si no se encuentra
        """
        parts = file_path.parts
        
        # Buscar partes numéricas que puedan ser año/mes/día
        year_parts = [p for p in parts if p.isdigit() and len(p) == 4]
        month_parts = [p for p in parts if p.isdigit() and len(p) == 1 or len(p) == 2 and int(p) <= 12]
        day_parts = [p for p in parts if p.isdigit() and len(p) == 1 or len(p) == 2 and int(p) <= 31]
        
        if year_parts and month_parts and day_parts:
            return [year_parts[0], month_parts[0], day_parts[0]]
        
        # Intentar extraer de nombre de archivo (formato: YYYY-MM-DD-HH)
        for part in parts:
            if "-" in part:
                date_parts = part.split("-")
                if len(date_parts) >= 3 and date_parts[0].isdigit() and len(date_parts[0]) == 4:
                    return date_parts[:3]
        
        return None
    
    def create_repo_activity_metrics(self, files_dict: Dict[str, List[Path]]) -> bool:
        """
        Crea métricas de actividad por repositorio
        
        Args:
            files_dict: Diccionario con archivos por tipo
            
        Returns:
            True si se procesa correctamente, False en caso contrario
        """
        try:
            logger.info("Generando métricas de actividad por repositorio")
            
            # Crear DataFrame para métricas de repositorio
            metrics_df = pd.DataFrame()
            
            # 1. Cargar datos de eventos para extraer información básica de repos
            events_files = files_dict.get('events', [])
            repo_info_df = None
            
            for file_path in tqdm(events_files, desc="Cargando datos de eventos"):
                try:
                    df = pd.read_parquet(file_path)
                    if repo_info_df is None:
                        repo_info_df = df[['repo_id', 'repo_name']].drop_duplicates()
                    else:
                        repo_info_df = pd.concat([
                            repo_info_df,
                            df[['repo_id', 'repo_name']].drop_duplicates()
                        ]).drop_duplicates()
                except Exception as e:
                    logger.warning(f"Error al leer archivo {file_path}: {e}")
            
            if repo_info_df is None or len(repo_info_df) == 0:
                logger.error("No se encontraron datos de repositorios")
                return False
                
            # Eliminar duplicados y configurar índice
            repo_info_df = repo_info_df.drop_duplicates('repo_id')
            metrics_df = repo_info_df.copy()
            
            # 2. Contar eventos por repositorio
            event_counts = {}
            
            # a. Contar eventos generales por repo
            for file_path in tqdm(events_files, desc="Contando eventos por repositorio"):
                try:
                    df = pd.read_parquet(file_path)
                    counts = df.groupby('repo_id').size()
                    
                    for repo_id, count in counts.items():
                        event_counts[repo_id] = event_counts.get(repo_id, 0) + count
                except Exception as e:
                    logger.warning(f"Error al procesar archivo {file_path}: {e}")
            
            # Agregar conteos al DataFrame
            metrics_df['total_events'] = metrics_df['repo_id'].map(lambda x: event_counts.get(x, 0))
            
            # 3. Contar Push Events por repositorio
            push_counts = {}
            commit_counts = {}
            authors = {}
            
            # a. Analizar eventos de push
            for file_path in tqdm(files_dict.get('push_events', []), desc="Analizando eventos de push"):
                try:
                    df = pd.read_parquet(file_path)
                    
                    # Contar pushes por repo
                    push_by_repo = df.groupby('repo_id')['event_id'].nunique()
                    for repo_id, count in push_by_repo.items():
                        push_counts[repo_id] = push_counts.get(repo_id, 0) + count
                    
                    # Contar commits por repo
                    if 'commit_sha' in df.columns:
                        commit_by_repo = df.groupby('repo_id')['commit_sha'].nunique()
                        for repo_id, count in commit_by_repo.items():
                            commit_counts[repo_id] = commit_counts.get(repo_id, 0) + count
                    
                    # Contar autores únicos
                    if 'commit_author_name' in df.columns and 'commit_author_email' in df.columns:
                        df['author_key'] = df['commit_author_name'] + "/" + df['commit_author_email']
                        authors_by_repo = df.groupby('repo_id')['author_key'].nunique()
                        
                        for repo_id, count in authors_by_repo.items():
                            if repo_id not in authors:
                                authors[repo_id] = set()
                            # No podemos sumar directamente, necesitamos los valores para deduplicar
                            author_values = df[df['repo_id'] == repo_id]['author_key'].unique()
                            authors[repo_id].update(author_values)
                except Exception as e:
                    logger.warning(f"Error al procesar archivo push {file_path}: {e}")
            
            # Agregar métricas de push al DataFrame
            metrics_df['push_count'] = metrics_df['repo_id'].map(lambda x: push_counts.get(x, 0))
            metrics_df['commit_count'] = metrics_df['repo_id'].map(lambda x: commit_counts.get(x, 0))
            metrics_df['author_count'] = metrics_df['repo_id'].map(lambda x: len(authors.get(x, set())))
            
            # 4. Contar Pull Requests por repositorio
            pr_counts = {}
            pr_merged = {}
            
            # a. Analizar eventos de pull request
            for file_path in tqdm(files_dict.get('pull_request_events', []), desc="Analizando pull requests"):
                try:
                    df = pd.read_parquet(file_path)
                    
                    # Contar PRs por repo (considerando números únicos)
                    if 'pr_number' in df.columns:
                        pr_by_repo = df.groupby('repo_id')['pr_number'].nunique()
                        for repo_id, count in pr_by_repo.items():
                            pr_counts[repo_id] = pr_counts.get(repo_id, 0) + count
                    
                    # Contar PRs merged por repo
                    if 'pr_merged' in df.columns:
                        merged = df[df['pr_merged'] == True]
                        if len(merged) > 0:
                            merged_by_repo = merged.groupby('repo_id')['pr_number'].nunique()
                            for repo_id, count in merged_by_repo.items():
                                pr_merged[repo_id] = pr_merged.get(repo_id, 0) + count
                except Exception as e:
                    logger.warning(f"Error al procesar archivo PR {file_path}: {e}")
            
            # Agregar métricas de PR al DataFrame
            metrics_df['pr_count'] = metrics_df['repo_id'].map(lambda x: pr_counts.get(x, 0))
            metrics_df['pr_merged_count'] = metrics_df['repo_id'].map(lambda x: pr_merged.get(x, 0))
            
            # 5. Contar Issues por repositorio
            issue_counts = {}
            issue_closed = {}
            
            # a. Analizar eventos de issues
            for file_path in tqdm(files_dict.get('issue_events', []), desc="Analizando issues"):
                try:
                    df = pd.read_parquet(file_path)
                    
                    # Contar issues por repo
                    if 'issue_number' in df.columns:
                        issues_by_repo = df.groupby('repo_id')['issue_number'].nunique()
                        for repo_id, count in issues_by_repo.items():
                            issue_counts[repo_id] = issue_counts.get(repo_id, 0) + count
                    
                    # Contar issues cerrados
                    if 'action' in df.columns and 'issue_number' in df.columns:
                        closed = df[df['action'] == 'closed']
                        if len(closed) > 0:
                            closed_by_repo = closed.groupby('repo_id')['issue_number'].nunique()
                            for repo_id, count in closed_by_repo.items():
                                issue_closed[repo_id] = issue_closed.get(repo_id, 0) + count
                except Exception as e:
                    logger.warning(f"Error al procesar archivo issues {file_path}: {e}")
            
            # Agregar métricas de issues al DataFrame
            metrics_df['issue_count'] = metrics_df['repo_id'].map(lambda x: issue_counts.get(x, 0))
            metrics_df['issue_closed_count'] = metrics_df['repo_id'].map(lambda x: issue_closed.get(x, 0))
            
            # 6. Calcular métricas derivadas
            metrics_df['pr_merge_ratio'] = (
                metrics_df['pr_merged_count'] / metrics_df['pr_count']
            ).fillna(0)
            
            metrics_df['issue_close_ratio'] = (
                metrics_df['issue_closed_count'] / metrics_df['issue_count']
            ).fillna(0)
            
            metrics_df['commits_per_push'] = (
                metrics_df['commit_count'] / metrics_df['push_count']
            ).fillna(0)
            
            # Calcular un score de actividad simplificado 
            # (peso de eventos normalizados por su importancia relativa)
            metrics_df['activity_score'] = (
                metrics_df['commit_count'] * 1 + 
                metrics_df['pr_count'] * 5 + 
                metrics_df['issue_count'] * 3 +
                metrics_df['total_events'] * 0.1
            )
            
            # 7. Guardar resultados
            output_path = REPO_METRICS_DIR / "repo_activity_metrics.parquet"
            metrics_df.to_parquet(output_path, index=False)
            
            # También guardar versión para los Top-N
            top_repos = metrics_df.nlargest(1000, 'activity_score')
            top_output_path = TOPN_DIR / "top_1000_active_repos.parquet"
            top_repos.to_parquet(top_output_path, index=False)
            
            logger.info(f"Métricas de repositorio generadas: {len(metrics_df)} repositorios analizados")
            logger.info(f"Resultados guardados en {output_path} y {top_output_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error al generar métricas de repositorio: {e}", exc_info=True)
            return False
    
    def create_user_contribution_metrics(self, files_dict: Dict[str, List[Path]]) -> bool:
        """
        Crea métricas de contribución por usuario
        
        Args:
            files_dict: Diccionario con archivos por tipo
            
        Returns:
            True si se procesa correctamente, False en caso contrario
        """
        try:
            logger.info("Generando métricas de contribución por usuario")
            
            # Crear DataFrame para métricas de usuario
            metrics_df = pd.DataFrame()
            
            # 1. Cargar datos de eventos para extraer información básica de usuarios
            events_files = files_dict.get('events', [])
            user_info_df = None
            
            for file_path in tqdm(events_files, desc="Cargando datos de usuarios"):
                try:
                    df = pd.read_parquet(file_path)
                    if user_info_df is None:
                        user_info_df = df[['actor_id', 'actor_login']].drop_duplicates()
                    else:
                        user_info_df = pd.concat([
                            user_info_df,
                            df[['actor_id', 'actor_login']].drop_duplicates()
                        ]).drop_duplicates()
                except Exception as e:
                    logger.warning(f"Error al leer archivo {file_path}: {e}")
            
            if user_info_df is None or len(user_info_df) == 0:
                logger.error("No se encontraron datos de usuarios")
                return False
                
            # Eliminar duplicados y configurar métrica base
            user_info_df = user_info_df.drop_duplicates('actor_id')
            metrics_df = user_info_df.copy()
            
            # 2. Contar eventos por usuario
            event_counts = {}
            repo_interactions = {}
            
            for file_path in tqdm(events_files, desc="Contando eventos por usuario"):
                try:
                    df = pd.read_parquet(file_path)
                    
                    # Contar eventos por usuario
                    counts = df.groupby('actor_id').size()
                    for user_id, count in counts.items():
                        event_counts[user_id] = event_counts.get(user_id, 0) + count
                    
                    # Contar repos únicos por usuario
                    user_repos = df.groupby('actor_id')['repo_id'].nunique()
                    for user_id, count in user_repos.items():
                        if user_id not in repo_interactions:
                            repo_interactions[user_id] = set()
                        # Obtener los repos únicos y agregarlos al set
                        repos = df[df['actor_id'] == user_id]['repo_id'].unique()
                        repo_interactions[user_id].update(repos)
                except Exception as e:
                    logger.warning(f"Error al procesar archivo {file_path}: {e}")
            
            # Agregar conteos al DataFrame
            metrics_df['total_events'] = metrics_df['actor_id'].map(lambda x: event_counts.get(x, 0))
            metrics_df['repos_contributed'] = metrics_df['actor_id'].map(lambda x: len(repo_interactions.get(x, set())))
            
            # 3. Contar contribuciones por tipo de evento
            # a. Push events
            push_counts = {}
            commit_counts = {}
            
            for file_path in tqdm(files_dict.get('push_events', []), desc="Analizando pushes por usuario"):
                try:
                    df = pd.read_parquet(file_path)
                    
                    # Contar pushes por usuario
                    push_by_user = df.groupby('actor_id')['event_id'].nunique()
                    for user_id, count in push_by_user.items():
                        push_counts[user_id] = push_counts.get(user_id, 0) + count
                    
                    # Contar commits por usuario
                    if 'commit_sha' in df.columns:
                        commit_by_user = df.groupby('actor_id')['commit_sha'].nunique()
                        for user_id, count in commit_by_user.items():
                            commit_counts[user_id] = commit_counts.get(user_id, 0) + count
                except Exception as e:
                    logger.warning(f"Error al procesar archivo push {file_path}: {e}")
            
            # b. Pull request events
            pr_counts = {}
            pr_merged = {}
            
            for file_path in tqdm(files_dict.get('pull_request_events', []), desc="Analizando PRs por usuario"):
                try:
                    df = pd.read_parquet(file_path)
                    
                    # Contar PRs por usuario
                    if 'pr_number' in df.columns:
                        pr_by_user = df.groupby('actor_id')['pr_number'].nunique()
                        for user_id, count in pr_by_user.items():
                            pr_counts[user_id] = pr_counts.get(user_id, 0) + count
                    
                    # Contar PRs merged por usuario
                    if 'pr_merged' in df.columns:
                        merged = df[df['pr_merged'] == True]
                        if len(merged) > 0:
                            merged_by_user = merged.groupby('actor_id')['pr_number'].nunique()
                            for user_id, count in merged_by_user.items():
                                pr_merged[user_id] = pr_merged.get(user_id, 0) + count
                except Exception as e:
                    logger.warning(f"Error al procesar archivo PR {file_path}: {e}")
            
            # c. Issue events
            issue_counts = {}
            issue_closed = {}
            
            for file_path in tqdm(files_dict.get('issue_events', []), desc="Analizando issues por usuario"):
                try:
                    df = pd.read_parquet(file_path)
                    
                    # Contar issues por usuario
                    if 'issue_number' in df.columns:
                        issues_by_user = df.groupby('actor_id')['issue_number'].nunique()
                        for user_id, count in issues_by_user.items():
                            issue_counts[user_id] = issue_counts.get(user_id, 0) + count
                    
                    # Contar issues cerrados
                    if 'action' in df.columns and 'issue_number' in df.columns:
                        closed = df[df['action'] == 'closed']
                        if len(closed) > 0:
                            closed_by_user = closed.groupby('actor_id')['issue_number'].nunique()
                            for user_id, count in closed_by_user.items():
                                issue_closed[user_id] = issue_closed.get(user_id, 0) + count
                except Exception as e:
                    logger.warning(f"Error al procesar archivo issues {file_path}: {e}")
            
            # d. Comment events
            comment_counts = {}
            
            for file_path in tqdm(files_dict.get('comment_events', []), desc="Analizando comentarios por usuario"):
                try:
                    df = pd.read_parquet(file_path)
                    
                    # Contar comentarios por usuario
                    if 'comment_id' in df.columns:
                        comments_by_user = df.groupby('actor_id')['comment_id'].nunique()
                        for user_id, count in comments_by_user.items():
                            comment_counts[user_id] = comment_counts.get(user_id, 0) + count
                    elif 'event_id' in df.columns:
                        comments_by_user = df.groupby('actor_id').size()
                        for user_id, count in comments_by_user.items():
                            comment_counts[user_id] = comment_counts.get(user_id, 0) + count
                except Exception as e:
                    logger.warning(f"Error al procesar archivo comentarios {file_path}: {e}")
            
            # Agregar todas las métricas al DataFrame
            metrics_df['push_count'] = metrics_df['actor_id'].map(lambda x: push_counts.get(x, 0))
            metrics_df['commit_count'] = metrics_df['actor_id'].map(lambda x: commit_counts.get(x, 0))
            metrics_df['pr_count'] = metrics_df['actor_id'].map(lambda x: pr_counts.get(x, 0))
            metrics_df['pr_merged_count'] = metrics_df['actor_id'].map(lambda x: pr_merged.get(x, 0))
            metrics_df['issue_count'] = metrics_df['actor_id'].map(lambda x: issue_counts.get(x, 0))
            metrics_df['issue_closed_count'] = metrics_df['actor_id'].map(lambda x: issue_closed.get(x, 0))
            metrics_df['comment_count'] = metrics_df['actor_id'].map(lambda x: comment_counts.get(x, 0))
            
            # 4. Calcular métricas derivadas
            metrics_df['pr_merge_ratio'] = (
                metrics_df['pr_merged_count'] / metrics_df['pr_count']
            ).fillna(0)
            
            metrics_df['issue_close_ratio'] = (
                metrics_df['issue_closed_count'] / metrics_df['issue_count']
            ).fillna(0)
            
            metrics_df['commits_per_push'] = (
                metrics_df['commit_count'] / metrics_df['push_count']
            ).fillna(0)
            
            # Calcular un score de contribución
            metrics_df['contribution_score'] = (
                metrics_df['commit_count'] * 1 + 
                metrics_df['pr_count'] * 5 + 
                metrics_df['pr_merged_count'] * 2 +
                metrics_df['issue_count'] * 2 +
                metrics_df['comment_count'] * 0.5
            )
            
            # 5. Guardar resultados
            output_path = USER_METRICS_DIR / "user_contribution_metrics.parquet"
            metrics_df.to_parquet(output_path, index=False)
            
            # También guardar versión para los Top-N
            top_users = metrics_df.nlargest(1000, 'contribution_score')
            top_output_path = TOPN_DIR / "top_1000_contributors.parquet"
            top_users.to_parquet(top_output_path, index=False)
            
            logger.info(f"Métricas de contribución generadas: {len(metrics_df)} usuarios analizados")
            logger.info(f"Resultados guardados en {output_path} y {top_output_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error al generar métricas de contribución: {e}", exc_info=True)
            return False
    
    def create_time_based_metrics(self, files_dict: Dict[str, List[Path]]) -> bool:
        """
        Crea métricas basadas en tiempo (por hora, día, etc.)
        
        Args:
            files_dict: Diccionario con archivos por tipo
            
        Returns:
            True si se procesa correctamente, False en caso contrario
        """
        try:
            logger.info("Generando métricas basadas en tiempo")
            
            # 1. Consolidar todos los eventos por timestamp
            events_df = None
            events_files = files_dict.get('events', [])
            
            for file_path in tqdm(events_files, desc="Cargando datos de eventos para análisis temporal"):
                try:
                    df = pd.read_parquet(file_path)
                    
                    # Asegurarse de que created_at es datetime
                    if 'created_at' in df.columns:
                        df['created_at'] = pd.to_datetime(df['created_at'])
                        
                        # Crear columnas temporales
                        df['date'] = df['created_at'].dt.date
                        df['hour'] = df['created_at'].dt.hour
                        df['day_of_week'] = df['created_at'].dt.dayofweek
                        df['month'] = df['created_at'].dt.month
                        df['year'] = df['created_at'].dt.year
                        
                        # Agregar al DataFrame principal
                        if events_df is None:
                            events_df = df[['id', 'type', 'created_at', 'date', 'hour', 'day_of_week', 'month', 'year', 'actor_id', 'repo_id']]
                        else:
                            events_df = pd.concat([
                                events_df,
                                df[['id', 'type', 'created_at', 'date', 'hour', 'day_of_week', 'month', 'year', 'actor_id', 'repo_id']]
                            ])
                except Exception as e:
                    logger.warning(f"Error al leer archivo temporal {file_path}: {e}")
            
            if events_df is None or len(events_df) == 0:
                logger.error("No se encontraron datos de eventos para análisis temporal")
                return False
            
            # 2. Crear agregaciones por diferentes dimensiones temporales
            # a. Por hora del día
            hourly_agg = events_df.groupby('hour').size().reset_index(name='event_count')
            hourly_agg['hour_str'] = hourly_agg['hour'].apply(lambda x: f"{x:02d}:00")
            
            # b. Por día de la semana
            day_names = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
            daily_agg = events_df.groupby('day_of_week').size().reset_index(name='event_count')
            daily_agg['day_name'] = daily_agg['day_of_week'].apply(lambda x: day_names[x])
            
            # c. Por fecha
            date_agg = events_df.groupby('date').size().reset_index(name='event_count')
            date_agg['date_str'] = date_agg['date'].astype(str)
            
            # d. Por tipo de evento y hora
            type_hourly_agg = events_df.groupby(['type', 'hour']).size().reset_index(name='event_count')
            type_hourly_agg['hour_str'] = type_hourly_agg['hour'].apply(lambda x: f"{x:02d}:00")
            
            # e. Por tipo de evento y día de semana
            type_daily_agg = events_df.groupby(['type', 'day_of_week']).size().reset_index(name='event_count')
            type_daily_agg['day_name'] = type_daily_agg['day_of_week'].apply(lambda x: day_names[x])
            
            # 3. Guardar resultados
            hourly_output = TIME_METRICS_DIR / "hourly_activity.parquet"
            daily_output = TIME_METRICS_DIR / "daily_activity.parquet"
            date_output = TIME_METRICS_DIR / "date_activity.parquet"
            type_hourly_output = TIME_METRICS_DIR / "type_hourly_activity.parquet"
            type_daily_output = TIME_METRICS_DIR / "type_daily_activity.parquet"
            
            hourly_agg.to_parquet(hourly_output, index=False)
            daily_agg.to_parquet(daily_output, index=False)
            date_agg.to_parquet(date_output, index=False)
            type_hourly_agg.to_parquet(type_hourly_output, index=False)
            type_daily_agg.to_parquet(type_daily_output, index=False)
            
            logger.info(f"Métricas temporales generadas y guardadas en {TIME_METRICS_DIR}")
            
            # 4. Crear análisis de tendencias de actividad
            # Si hay suficientes días, crear análisis de tendencias
            unique_dates = events_df['date'].nunique()
            
            if unique_dates >= 2:
                # Tendencias diarias por tipo de evento
                trend_by_type = (
                    events_df.groupby(['date', 'type'])
                    .size()
                    .reset_index(name='event_count')
                )
                trend_by_type['date_str'] = trend_by_type['date'].astype(str)
                
                # Guardar tendencias
                trend_output = TIME_METRICS_DIR / "event_type_trends.parquet"
                trend_by_type.to_parquet(trend_output, index=False)
                
                logger.info(f"Análisis de tendencias generado y guardado en {trend_output}")
            else:
                logger.info("No hay suficientes días para generar análisis de tendencias")
            
            return True
            
        except Exception as e:
            logger.error(f"Error al generar métricas temporales: {e}", exc_info=True)
            return False
    
    def create_repo_user_relationships(self, files_dict: Dict[str, List[Path]]) -> bool:
        """
        Crea relaciones entre repositorios y usuarios
        
        Args:
            files_dict: Diccionario con archivos por tipo
            
        Returns:
            True si se procesa correctamente, False en caso contrario
        """
        try:
            logger.info("Generando relaciones entre usuarios y repositorios")
            
            # 1. Consolidar interacciones usuario-repositorio
            events_df = None
            events_files = files_dict.get('events', [])
            
            for file_path in tqdm(events_files, desc="Cargando datos de interacciones"):
                try:
                    df = pd.read_parquet(file_path)
                    if 'actor_id' in df.columns and 'repo_id' in df.columns and 'type' in df.columns:
                        interaction_data = df[['actor_id', 'actor_login', 'repo_id', 'repo_name', 'type']]
                        
                        if events_df is None:
                            events_df = interaction_data
                        else:
                            events_df = pd.concat([events_df, interaction_data])
                except Exception as e:
                    logger.warning(f"Error al leer archivo de interacciones {file_path}: {e}")
            
            if events_df is None or len(events_df) == 0:
                logger.error("No se encontraron datos de interacciones")
                return False
            
            # 2. Agregar interacciones por usuario-repositorio-tipo
            interaction_counts = (
                events_df.groupby(['actor_id', 'actor_login', 'repo_id', 'repo_name', 'type'])
                .size()
                .reset_index(name='interaction_count')
            )
            
            # 3. Crear una vista desnormalizada de las interacciones
            pivot_interactions = (
                interaction_counts
                .pivot_table(
                    index=['actor_id', 'actor_login', 'repo_id', 'repo_name'],
                    columns='type',
                    values='interaction_count',
                    aggfunc='sum',
                    fill_value=0
                )
                .reset_index()
            )
            
            # Añadir total de interacciones
            pivot_interactions['total_interactions'] = pivot_interactions.select_dtypes(include=['number']).sum(axis=1)
            
            # 4. Crear una vista de top contribuyentes por repositorio
            top_contributors_by_repo = (
                pivot_interactions
                .groupby('repo_id')
                .apply(lambda x: x.nlargest(10, 'total_interactions'))
                .reset_index(drop=True)
            )
            
            # 5. Guardar resultados
            interactions_output = GOLD_DATA_DIR / "user_repo_interactions.parquet"
            top_contributors_output = TOPN_DIR / "top_contributors_by_repo.parquet"
            
            pivot_interactions.to_parquet(interactions_output, index=False)
            top_contributors_by_repo.to_parquet(top_contributors_output, index=False)
            
            logger.info(f"Relaciones usuario-repositorio generadas: {len(pivot_interactions)} interacciones")
            logger.info(f"Resultados guardados en {interactions_output} y {top_contributors_output}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error al generar relaciones usuario-repositorio: {e}", exc_info=True)
            return False
    
    def create_consolidated_metrics(self) -> bool:
        """
        Crea un archivo consolidado con todas las métricas principales
        """
        try:
            logger.info("Generando dashboard de métricas consolidadas")
            
            # 1. Métricas globales
            global_metrics = {}
            
            # a. Intentar cargar métricas de repositorios
            repo_path = REPO_METRICS_DIR / "repo_activity_metrics.parquet"
            if repo_path.exists():
                repo_df = pd.read_parquet(repo_path)
                global_metrics['total_repositories'] = len(repo_df)
                global_metrics['total_commits'] = repo_df['commit_count'].sum()
                global_metrics['total_prs'] = repo_df['pr_count'].sum()
                global_metrics['total_issues'] = repo_df['issue_count'].sum()
                global_metrics['avg_pr_merge_ratio'] = repo_df['pr_merge_ratio'].mean()
                global_metrics['avg_issue_close_ratio'] = repo_df['issue_close_ratio'].mean()
                
                # Top repositorios
                top5_repos = repo_df.nlargest(5, 'activity_score')[['repo_name', 'activity_score', 'commit_count', 'pr_count', 'issue_count']]
                global_metrics['top5_active_repos'] = top5_repos.to_dict('records')
            
            # b. Intentar cargar métricas de usuarios
            user_path = USER_METRICS_DIR / "user_contribution_metrics.parquet"
            if user_path.exists():
                user_df = pd.read_parquet(user_path)
                global_metrics['total_contributors'] = len(user_df)
                
                # Top contribuidores
                top5_users = user_df.nlargest(5, 'contribution_score')[['actor_login', 'contribution_score', 'commit_count', 'pr_count', 'issue_count']]
                global_metrics['top5_contributors'] = top5_users.to_dict('records')
            
            # c. Intentar cargar métricas temporales
            hourly_path = TIME_METRICS_DIR / "hourly_activity.parquet"
            if hourly_path.exists():
                hourly_df = pd.read_parquet(hourly_path)
                peak_hour = hourly_df.loc[hourly_df['event_count'].idxmax()]
                global_metrics['peak_activity_hour'] = peak_hour['hour_str']
            
            daily_path = TIME_METRICS_DIR / "daily_activity.parquet"
            if daily_path.exists():
                daily_df = pd.read_parquet(daily_path)
                peak_day = daily_df.loc[daily_df['event_count'].idxmax()]
                global_metrics['peak_activity_day'] = peak_day['day_name']
            
            # 2. Guardar métricas consolidadas
            if global_metrics:
                output_path = GOLD_DATA_DIR / "metrics_dashboard.json"
                with open(output_path, 'w') as f:
                    json.dump(global_metrics, f, indent=2, default=str)
                
                logger.info(f"Dashboard de métricas consolidadas guardado en {output_path}")
                return True
            else:
                logger.error("No se encontraron métricas para consolidar")
                return False
            
        except Exception as e:
            logger.error(f"Error al generar métricas consolidadas: {e}", exc_info=True)
            return False
    
    def run(self) -> Dict[str, bool]:
        """
        Ejecuta el procesamiento Gold completo
        
        Returns:
            Diccionario con {proceso: éxito}
        """
        results = {}
        
        # 1. Encontrar archivos silver
        logger.info("Buscando archivos Silver para procesar")
        files_dict = self.find_silver_files()
        
        # Verificar que hay archivos para procesar
        total_files = sum(len(files) for files in files_dict.values())
        if total_files == 0:
            logger.warning("No se encontraron archivos Silver para procesar")
            return results
        
        logger.info(f"Encontrados {total_files} archivos Silver para procesar")
        for file_type, files in files_dict.items():
            if files:
                logger.info(f"  - {file_type}: {len(files)} archivos")
        
        # 2. Ejecutar cada procesamiento de métricas
        # a. Métricas de repositorio
        results['repo_metrics'] = self.create_repo_activity_metrics(files_dict)
        
        # b. Métricas de contribución de usuarios
        results['user_metrics'] = self.create_user_contribution_metrics(files_dict)
        
        # c. Métricas basadas en tiempo
        results['time_metrics'] = self.create_time_based_metrics(files_dict)
        
        # d. Relaciones usuario-repositorio
        results['user_repo_relationships'] = self.create_repo_user_relationships(files_dict)
        
        # e. Consolidar todas las métricas en un dashboard
        results['consolidated_metrics'] = self.create_consolidated_metrics()
        
        # Resultados
        successful = sum(1 for success in results.values() if success)
        total = len(results)
        
        logger.info(f"Procesamiento Gold completado: {successful}/{total} procesos exitosos")
        
        return results
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """
        Obtiene estadísticas del procesamiento Gold
        
        Returns:
            Diccionario con estadísticas
        """
        stats = {
            'total_files': 0,
            'total_size_bytes': 0,
            'by_category': {}
        }
        
        # Categorías a revisar
        categories = {
            'repo_metrics': REPO_METRICS_DIR,
            'user_metrics': USER_METRICS_DIR,
            'time_metrics': TIME_METRICS_DIR,
            'top_n': TOPN_DIR,
            'root': self.output_dir
        }
        
        # Contar archivos por categoría
        for category, dir_path in categories.items():
            count = 0
            size = 0
            
            for file_path in dir_path.glob('**/*.*'):
                if file_path.is_file():
                    count += 1
                    size += file_path.stat().st_size
            
            stats['by_category'][category] = {
                'count': count,
                'size_bytes': size,
                'size_mb': size / (1024 * 1024)
            }
            
            # Sumar a los totales, pero solo de categorías no-root para evitar duplicados
            if category != 'root':
                stats['total_files'] += count
                stats['total_size_bytes'] += size
        
        stats['total_size_mb'] = stats['total_size_bytes'] / (1024 * 1024)
        
        return stats


def parse_arguments():
    """
    Parsea los argumentos de línea de comandos
    
    Returns:
        Argumentos parseados
    """
    parser = argparse.ArgumentParser(description="Procesador de datos Gold para GitHub Archive")
    
    parser.add_argument(
        "--input-dir",
        type=str,
        default=str(SILVER_DATA_DIR),
        help=f"Directorio con archivos silver (default: {SILVER_DATA_DIR})"
    )
    
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(GOLD_DATA_DIR),
        help=f"Directorio para guardar archivos gold (default: {GOLD_DATA_DIR})"
    )
    
    parser.add_argument(
        "--max-workers",
        type=int,
        default=3,
        help="Número máximo de trabajadores para procesamiento paralelo (default: 3)"
    )
    
    parser.add_argument(
        "--start-date",
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d").date(),
        help="Fecha de inicio para filtrar datos (formato: YYYY-MM-DD)"
    )
    
    parser.add_argument(
        "--end-date",
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d").date(),
        help="Fecha de fin para filtrar datos (formato: YYYY-MM-DD)"
    )
    
    return parser.parse_args()


def main():
    """Función principal"""
    try:
        # Parsear argumentos
        args = parse_arguments()
        
        # Configurar rango de fechas si se especificaron ambas
        date_range = None
        if args.start_date and args.end_date:
            date_range = (args.start_date, args.end_date)
        
        # Inicializar procesador
        processor = GoldProcessor(
            input_dir=Path(args.input_dir),
            output_dir=Path(args.output_dir),
            max_workers=args.max_workers,
            date_range=date_range
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
            print(f"\n✅ Procesamiento Gold completado exitosamente: {successful}/{total} procesos")
            print(f"📊 Total procesado: {stats.get('total_size_mb', 0):.2f} MB en {stats.get('total_files', 0)} archivos")
            print(f"📂 Archivos guardados en: {args.output_dir}")
            
            # Mostrar desglose por categoría
            for category, cat_stats in stats.get('by_category', {}).items():
                if category != 'root' and cat_stats.get('count', 0) > 0:
                    print(f"  - {category}: {cat_stats.get('count', 0)} archivos, {cat_stats.get('size_mb', 0):.2f} MB")
            
            return 0
        else:
            print(f"\n⚠️ Procesamiento Gold completado con advertencias: {successful}/{total} procesos")
            for process, success in results.items():
                if not success:
                    print(f"❌ Proceso fallido: {process}")
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