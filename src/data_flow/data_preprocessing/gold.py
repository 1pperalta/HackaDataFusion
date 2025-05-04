#!/usr/bin/env python
"""
GitHub Archive Gold Processor

Este script procesa los datos de la capa Silver de GitHub Archive
y los transforma para la capa Gold, aplicando agregaciones,
cálculos avanzados y preparando los datos para análisis y visualización.

La capa Gold contiene datos optimizados para análisis, con métricas
precomputadas, agregaciones y vistas analíticas listas para consumir.
"""

import os
import sys
import logging
import argparse
import datetime
import json
import glob
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
import time
from collections import defaultdict, Counter

import pandas as pd
import numpy as np
from tqdm import tqdm

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levellevel)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/gold_processor.log", mode='a')
    ]
)
logger = logging.getLogger("gold_processor")

# Directorios para datos
SILVER_DIR = Path("data/processed/silver")
GOLD_DIR = Path("data/processed/gold")

# Definir las tablas y métricas de la capa Gold
GOLD_TABLES = {
    "event_metrics": [
        "date", "hour", "total_events", "public_events", "private_events",
        "unique_actors", "unique_repos", "unique_orgs", "bot_percentage",
        "top_event_types", "processed_at"
    ],
    "actor_metrics": [
        "date", "actor_type", "total_actors", "active_actors", "new_actors",
        "avg_events_per_actor", "top_active_actors", "bot_percentage", "processed_at"
    ],
    "repo_metrics": [
        "date", "total_repos", "active_repos", "new_repos", "avg_events_per_repo",
        "top_active_repos", "top_languages", "processed_at"
    ],
    "language_trends": [
        "date", "language", "repo_count", "event_count", "actor_count", 
        "trend_score", "previous_trend_score", "change_percentage", "processed_at"
    ],
    "geographical_activity": [
        "date", "region_code", "country_code", "event_count", "actor_count", 
        "repo_count", "top_event_types", "processed_at"
    ],
    "time_based_activity": [
        "date", "hour", "weekday", "event_count", "unique_actors", 
        "unique_repos", "anomaly_score", "processed_at"
    ],
    "repository_popularity": [
        "date", "repo_id", "repo_name", "owner_login", "language", 
        "event_count", "unique_actors", "stars_delta", "forks_delta", 
        "engagement_score", "processed_at"
    ]
}


class GoldProcessor:
    """Procesador de capa Gold para eventos de GitHub"""
    
    def __init__(
        self,
        silver_dir: Path = SILVER_DIR,
        gold_dir: Path = GOLD_DIR,
        date_pattern: Optional[str] = None,
        use_csv: bool = True
    ):
        """
        Inicializa el procesador de capa Gold
        
        Args:
            silver_dir: Directorio con datos de capa Silver
            gold_dir: Directorio para datos de capa Gold
            date_pattern: Patrón de fecha opcional (YYYY/MM/DD)
            use_csv: Si se debe usar CSV en lugar de parquet
        """
        self.silver_dir = silver_dir
        self.gold_dir = gold_dir
        self.date_pattern = date_pattern
        self.use_csv = use_csv
        
        # Para almacenar datos temporalmente
        self.silver_cache = {
            "events": None,
            "actors": None,
            "repositories": None,
            "organizations": None,
            "payload_details": None
        }
        
        # Crear directorios si no existen
        self.gold_dir.mkdir(parents=True, exist_ok=True)
        for table in GOLD_TABLES:
            (self.gold_dir / table).mkdir(parents=True, exist_ok=True)
    
    def find_silver_files(self) -> Dict[str, List[Path]]:
        """
        Encuentra archivos en el directorio de datos Silver
        
        Returns:
            Diccionario con {tipo_tabla: [rutas_archivos]}
        """
        files = defaultdict(list)
        
        for table in self.silver_cache.keys():
            table_dir = self.silver_dir / table
            
            if not table_dir.exists():
                logger.warning(f"Directorio de tabla Silver no encontrado: {table_dir}")
                continue
            
            # Buscar archivos CSV y parquet
            if self.date_pattern:
                # Si hay un patrón de fecha, buscar archivos que coincidan
                csv_files = list(table_dir.glob(f"{self.date_pattern}*.csv"))
                parquet_files = list(table_dir.glob(f"{self.date_pattern}*.parquet"))
            else:
                # Si no hay patrón, buscar todos los archivos
                csv_files = list(table_dir.glob("*.csv"))
                parquet_files = list(table_dir.glob("*.parquet"))
            
            # Combinar listas
            table_files = csv_files + parquet_files
            files[table].extend(table_files)
            
            logger.info(f"Encontrados {len(table_files)} archivos para tabla {table}")
        
        return files
    
    def load_silver_data(self) -> bool:
        """
        Carga datos Silver en la caché para procesamiento
        
        Returns:
            True si se cargaron datos, False en caso contrario
        """
        silver_files = self.find_silver_files()
        
        for table, file_list in silver_files.items():
            if not file_list:
                logger.warning(f"No se encontraron archivos para tabla {table}")
                continue
            
            dfs = []
            for file_path in tqdm(file_list, desc=f"Cargando {table}"):
                try:
                    if file_path.suffix == '.csv':
                        df = pd.read_csv(file_path)
                    elif file_path.suffix == '.parquet':
                        df = pd.read_parquet(file_path)
                    else:
                        logger.warning(f"Formato de archivo no soportado: {file_path}")
                        continue
                    
                    # Inferir tipos para optimizar memoria
                    for col in df.columns:
                        if df[col].dtype == 'object' and col.endswith('_id'):
                            try:
                                df[col] = pd.to_numeric(df[col])
                            except (ValueError, TypeError):
                                pass
                    
                    dfs.append(df)
                except Exception as e:
                    logger.error(f"Error al cargar archivo {file_path}: {e}")
            
            if dfs:
                try:
                    # Combinar todos los DataFrames para esta tabla
                    combined_df = pd.concat(dfs, ignore_index=True)
                    
                    # Eliminar duplicados basados en ID
                    id_col = None
                    for col_candidate in [f"{table[:-1]}_id", "id", "event_id"]:
                        if col_candidate in combined_df.columns:
                            id_col = col_candidate
                            break
                    
                    if id_col:
                        logger.info(f"Eliminando duplicados de {table} basados en columna {id_col}")
                        combined_df = combined_df.drop_duplicates(subset=[id_col])
                    
                    # Almacenar en caché
                    self.silver_cache[table] = combined_df
                    logger.info(f"Cargados {len(combined_df)} registros para tabla {table}")
                except Exception as e:
                    logger.error(f"Error al procesar tabla {table}: {e}")
                    return False
        
        # Verificar que se hayan cargado datos
        if all(df is None for df in self.silver_cache.values()):
            logger.error("No se pudo cargar ningún dato de la capa Silver")
            return False
        
        return True
    
    def extract_date_components(self) -> pd.DataFrame:
        """
        Extrae componentes de fecha de los eventos para análisis temporal
        
        Returns:
            DataFrame con componentes de fecha
        """
        if self.silver_cache["events"] is None:
            logger.error("No hay datos de eventos disponibles")
            return pd.DataFrame()
        
        events_df = self.silver_cache["events"].copy()
        
        try:
            # Convertir created_at a datetime
            events_df['created_at'] = pd.to_datetime(events_df['created_at'], errors='coerce')
            
            # Extraer componentes de fecha
            events_df['date'] = events_df['created_at'].dt.date
            events_df['year'] = events_df['created_at'].dt.year
            events_df['month'] = events_df['created_at'].dt.month
            events_df['day'] = events_df['created_at'].dt.day
            events_df['hour'] = events_df['created_at'].dt.hour
            events_df['weekday'] = events_df['created_at'].dt.dayofweek
            events_df['weekday_name'] = events_df['created_at'].dt.day_name()
            
            return events_df
        except Exception as e:
            logger.error(f"Error al extraer componentes de fecha: {e}")
            return pd.DataFrame()
    
    def generate_event_metrics(self) -> pd.DataFrame:
        """
        Genera métricas agregadas por eventos
        
        Returns:
            DataFrame con métricas de eventos
        """
        if self.silver_cache["events"] is None:
            logger.error("No hay datos de eventos disponibles")
            return pd.DataFrame()
        
        try:
            # Obtener eventos con componentes de fecha
            events_df = self.extract_date_components()
            if events_df.empty:
                return pd.DataFrame()
            
            # Agrupar por fecha y hora
            grouped = events_df.groupby(['date', 'hour'])
            
            metrics = []
            current_time = datetime.datetime.now().isoformat()
            
            for (date, hour), group in tqdm(grouped, desc="Generando métricas de eventos"):
                # Contar eventos
                total_events = len(group)
                public_events = group['public'].sum() if 'public' in group.columns else 0
                private_events = total_events - public_events
                
                # Contar entidades únicas
                unique_actors = group['actor_id'].nunique() if 'actor_id' in group.columns else 0
                unique_repos = group['repo_id'].nunique() if 'repo_id' in group.columns else 0
                unique_orgs = group['org_id'].nunique() if 'org_id' in group.columns else 0
                
                # Calcular porcentaje de bots
                bot_events = group['is_bot'].sum() if 'is_bot' in group.columns else 0
                bot_percentage = (bot_events / total_events * 100) if total_events > 0 else 0
                
                # Obtener tipos de eventos más comunes
                top_events = group['event_type'].value_counts().head(5).to_dict()
                
                metrics.append({
                    'date': str(date),
                    'hour': hour,
                    'total_events': total_events,
                    'public_events': public_events,
                    'private_events': private_events,
                    'unique_actors': unique_actors,
                    'unique_repos': unique_repos,
                    'unique_orgs': unique_orgs,
                    'bot_percentage': bot_percentage,
                    'top_event_types': json.dumps(top_events),
                    'processed_at': current_time
                })
            
            return pd.DataFrame(metrics)
        
        except Exception as e:
            logger.error(f"Error al generar métricas de eventos: {e}")
            return pd.DataFrame()
    
    def generate_actor_metrics(self) -> pd.DataFrame:
        """
        Genera métricas agregadas por actores
        
        Returns:
            DataFrame con métricas de actores
        """
        if self.silver_cache["events"] is None or self.silver_cache["actors"] is None:
            logger.error("No hay datos suficientes para generar métricas de actores")
            return pd.DataFrame()
        
        try:
            # Obtener eventos con componentes de fecha
            events_df = self.extract_date_components()
            actors_df = self.silver_cache["actors"]
            
            if events_df.empty or actors_df.empty:
                return pd.DataFrame()
            
            # Filtrar eventos por actor_id válido
            events_df = events_df[events_df['actor_id'].notna()]
            
            # Agrupar por fecha
            grouped = events_df.groupby('date')
            
            metrics = []
            current_time = datetime.datetime.now().isoformat()
            
            for date, group in tqdm(grouped, desc="Generando métricas de actores"):
                # Contar actores
                total_actors = actors_df.shape[0]
                active_actors = group['actor_id'].nunique()
                
                # Calcular promedio de eventos por actor
                events_per_actor = group.groupby('actor_id').size()
                avg_events = events_per_actor.mean() if not events_per_actor.empty else 0
                
                # Obtener actores más activos (top 10)
                top_actors = events_per_actor.nlargest(10).to_dict()
                
                # Calcular porcentaje de bots
                bot_actors = (actors_df['is_bot'] == True).sum() if 'is_bot' in actors_df.columns else 0
                bot_percentage = (bot_actors / total_actors * 100) if total_actors > 0 else 0
                
                # Identificar actores nuevos (comparación con first_seen_at)
                try:
                    actors_df['first_seen_at'] = pd.to_datetime(actors_df['first_seen_at'], errors='coerce')
                    new_actors = actors_df[actors_df['first_seen_at'].dt.date == date].shape[0]
                except:
                    new_actors = 0
                
                # Separar métricas por tipo de actor
                for actor_type in ['User', 'Organization', 'Bot']:
                    type_filter = (actors_df['actor_type'] == actor_type) if 'actor_type' in actors_df.columns else (
                        actors_df['is_bot'] == True if actor_type == 'Bot' else 
                        actors_df['is_bot'] == False
                    )
                    
                    type_actors = actors_df[type_filter].shape[0] if type_filter is not None else 0
                    type_active = len(set(group['actor_id']).intersection(set(actors_df[type_filter]['actor_id']))) if type_filter is not None else 0
                    
                    metrics.append({
                        'date': str(date),
                        'actor_type': actor_type,
                        'total_actors': type_actors,
                        'active_actors': type_active,
                        'new_actors': new_actors if actor_type == 'User' else 0,  # Simplificación
                        'avg_events_per_actor': avg_events if actor_type == 'User' else 0,  # Simplificación
                        'top_active_actors': json.dumps(top_actors) if actor_type == 'User' else '{}',
                        'bot_percentage': bot_percentage if actor_type == 'User' else 0,
                        'processed_at': current_time
                    })
            
            return pd.DataFrame(metrics)
        
        except Exception as e:
            logger.error(f"Error al generar métricas de actores: {e}")
            return pd.DataFrame()
    
    def generate_repo_metrics(self) -> pd.DataFrame:
        """
        Genera métricas agregadas por repositorios
        
        Returns:
            DataFrame con métricas de repositorios
        """
        if self.silver_cache["events"] is None or self.silver_cache["repositories"] is None:
            logger.error("No hay datos suficientes para generar métricas de repositorios")
            return pd.DataFrame()
        
        try:
            # Obtener eventos con componentes de fecha
            events_df = self.extract_date_components()
            repos_df = self.silver_cache["repositories"]
            
            if events_df.empty or repos_df.empty:
                return pd.DataFrame()
            
            # Filtrar eventos por repo_id válido
            events_df = events_df[events_df['repo_id'].notna()]
            
            # Agrupar por fecha
            grouped = events_df.groupby('date')
            
            metrics = []
            current_time = datetime.datetime.now().isoformat()
            
            for date, group in tqdm(grouped, desc="Generando métricas de repositorios"):
                # Contar repositorios
                total_repos = repos_df.shape[0]
                active_repos = group['repo_id'].nunique()
                
                # Calcular promedio de eventos por repo
                events_per_repo = group.groupby('repo_id').size()
                avg_events = events_per_repo.mean() if not events_per_repo.empty else 0
                
                # Obtener repos más activos (top 10)
                top_repos_counts = events_per_repo.nlargest(10)
                top_repos = {}
                for repo_id, count in top_repos_counts.items():
                    repo_name = repos_df[repos_df['repo_id'] == repo_id]['repo_name'].iloc[0] if not repos_df[repos_df['repo_id'] == repo_id].empty else str(repo_id)
                    top_repos[str(repo_name)] = int(count)
                
                # Contar lenguajes (top 5)
                top_languages = {}
                if 'language' in repos_df.columns:
                    active_repo_ids = set(group['repo_id'])
                    active_repos_df = repos_df[repos_df['repo_id'].isin(active_repo_ids)]
                    
                    if not active_repos_df.empty and 'language' in active_repos_df.columns:
                        language_counts = active_repos_df['language'].value_counts().head(5).to_dict()
                        top_languages = {str(k): int(v) for k, v in language_counts.items() if k and pd.notna(k)}
                
                # Identificar repos nuevos (comparación con first_seen_at)
                try:
                    repos_df['first_seen_at'] = pd.to_datetime(repos_df['first_seen_at'], errors='coerce')
                    new_repos = repos_df[repos_df['first_seen_at'].dt.date == date].shape[0]
                except:
                    new_repos = 0
                
                metrics.append({
                    'date': str(date),
                    'total_repos': total_repos,
                    'active_repos': active_repos,
                    'new_repos': new_repos,
                    'avg_events_per_repo': avg_events,
                    'top_active_repos': json.dumps(top_repos),
                    'top_languages': json.dumps(top_languages),
                    'processed_at': current_time
                })
            
            return pd.DataFrame(metrics)
        
        except Exception as e:
            logger.error(f"Error al generar métricas de repositorios: {e}")
            return pd.DataFrame()
    
    def generate_language_trends(self) -> pd.DataFrame:
        """
        Genera tendencias de lenguajes de programación
        
        Returns:
            DataFrame con tendencias de lenguajes
        """
        if self.silver_cache["events"] is None or self.silver_cache["repositories"] is None:
            logger.error("No hay datos suficientes para generar tendencias de lenguajes")
            return pd.DataFrame()
        
        try:
            # Obtener eventos con componentes de fecha
            events_df = self.extract_date_components()
            repos_df = self.silver_cache["repositories"].copy()
            
            if events_df.empty or repos_df.empty or 'language' not in repos_df.columns:
                return pd.DataFrame()
            
            # Filtrar eventos por repo_id válido
            events_df = events_df[events_df['repo_id'].notna()]
            
            # Unir eventos con repos para obtener lenguajes
            merged = events_df.merge(repos_df[['repo_id', 'language']], on='repo_id', how='left')
            
            # Filtrar lenguajes no válidos
            merged = merged[merged['language'].notna() & (merged['language'] != '')]
            
            # Agrupar por fecha y lenguaje
            grouped = merged.groupby(['date', 'language'])
            
            trends = []
            current_time = datetime.datetime.now().isoformat()
            previous_day_counts = {}  # Para calcular cambio respecto al día anterior
            
            for (date, language), group in tqdm(grouped, desc="Generando tendencias de lenguajes"):
                # Contar eventos y entidades
                event_count = len(group)
                repo_count = group['repo_id'].nunique()
                actor_count = group['actor_id'].nunique()
                
                # Calcular puntuación de tendencia (ponderación)
                trend_score = (event_count * 0.5) + (repo_count * 0.3) + (actor_count * 0.2)
                
                # Calcular cambio respecto al día anterior
                previous_score = previous_day_counts.get(language, 0)
                change_percentage = ((trend_score - previous_score) / previous_score * 100) if previous_score > 0 else 0
                
                trends.append({
                    'date': str(date),
                    'language': language,
                    'repo_count': repo_count,
                    'event_count': event_count,
                    'actor_count': actor_count,
                    'trend_score': trend_score,
                    'previous_trend_score': previous_score,
                    'change_percentage': change_percentage,
                    'processed_at': current_time
                })
                
                # Actualizar conteos del día anterior
                previous_day_counts[language] = trend_score
            
            return pd.DataFrame(trends)
        
        except Exception as e:
            logger.error(f"Error al generar tendencias de lenguajes: {e}")
            return pd.DataFrame()
    
    def generate_time_based_activity(self) -> pd.DataFrame:
        """
        Genera patrones de actividad basados en tiempo
        
        Returns:
            DataFrame con actividad por hora y día de la semana
        """
        if self.silver_cache["events"] is None:
            logger.error("No hay datos de eventos disponibles")
            return pd.DataFrame()
        
        try:
            # Obtener eventos con componentes de fecha
            events_df = self.extract_date_components()
            if events_df.empty:
                return pd.DataFrame()
            
            # Agrupar por fecha, hora y día de la semana
            grouped = events_df.groupby(['date', 'hour', 'weekday'])
            
            activities = []
            current_time = datetime.datetime.now().isoformat()
            
            # Para detectar anomalías, necesitamos medias históricas
            hour_baselines = events_df.groupby('hour')['event_id'].count().to_dict()
            weekday_baselines = events_df.groupby('weekday')['event_id'].count().to_dict()
            
            for (date, hour, weekday), group in tqdm(grouped, desc="Generando actividad por tiempo"):
                # Contar eventos y entidades únicas
                event_count = len(group)
                unique_actors = group['actor_id'].nunique() if 'actor_id' in group.columns else 0
                unique_repos = group['repo_id'].nunique() if 'repo_id' in group.columns else 0
                
                # Calcular puntuación de anomalía
                hour_avg = hour_baselines.get(hour, 1)
                weekday_avg = weekday_baselines.get(weekday, 1)
                expected = (hour_avg + weekday_avg) / 2
                anomaly_score = (event_count - expected) / expected if expected > 0 else 0
                
                activities.append({
                    'date': str(date),
                    'hour': hour,
                    'weekday': weekday,
                    'event_count': event_count,
                    'unique_actors': unique_actors,
                    'unique_repos': unique_repos,
                    'anomaly_score': anomaly_score,
                    'processed_at': current_time
                })
            
            return pd.DataFrame(activities)
        
        except Exception as e:
            logger.error(f"Error al generar actividad por tiempo: {e}")
            return pd.DataFrame()
    
    def generate_repository_popularity(self) -> pd.DataFrame:
        """
        Genera métricas de popularidad de repositorios
        
        Returns:
            DataFrame con métricas de popularidad
        """
        if self.silver_cache["events"] is None or self.silver_cache["repositories"] is None:
            logger.error("No hay datos suficientes para generar popularidad de repositorios")
            return pd.DataFrame()
        
        try:
            # Obtener eventos con componentes de fecha
            events_df = self.extract_date_components()
            repos_df = self.silver_cache["repositories"].copy()
            
            if events_df.empty or repos_df.empty:
                return pd.DataFrame()
            
            # Filtrar eventos por repo_id válido y unir con repos
            events_df = events_df[events_df['repo_id'].notna()]
            merged = events_df.merge(repos_df, on='repo_id', how='left')
            
            # Agrupar por fecha y repo
            grouped = merged.groupby(['date', 'repo_id'])
            
            popularity = []
            current_time = datetime.datetime.now().isoformat()
            
            for (date, repo_id), group in tqdm(grouped, desc="Generando popularidad de repos"):
                # Identificar el repositorio
                repo_row = repos_df[repos_df['repo_id'] == repo_id]
                if repo_row.empty:
                    continue
                
                repo_name = repo_row['repo_name'].iloc[0] if 'repo_name' in repo_row else str(repo_id)
                owner_login = repo_row['owner_login'].iloc[0] if 'owner_login' in repo_row else ""
                language = repo_row['language'].iloc[0] if 'language' in repo_row else ""
                
                # Contar eventos y actores únicos
                event_count = len(group)
                unique_actors = group['actor_id'].nunique()
                
                # Estimar cambios en stars y forks usando tipos de eventos
                stars_delta = group[group['event_type'] == 'WatchEvent'].shape[0]
                forks_delta = group[group['event_type'] == 'ForkEvent'].shape[0]
                
                # Calcular puntuación de engagement
                engagement_score = (event_count * 0.4) + (unique_actors * 0.3) + (stars_delta * 0.2) + (forks_delta * 0.1)
                
                popularity.append({
                    'date': str(date),
                    'repo_id': repo_id,
                    'repo_name': repo_name,
                    'owner_login': owner_login,
                    'language': language,
                    'event_count': event_count,
                    'unique_actors': unique_actors,
                    'stars_delta': stars_delta,
                    'forks_delta': forks_delta,
                    'engagement_score': engagement_score,
                    'processed_at': current_time
                })
            
            # Ordenar por puntuación de engagement
            pop_df = pd.DataFrame(popularity)
            if not pop_df.empty:
                pop_df = pop_df.sort_values('engagement_score', ascending=False)
            
            return pop_df
        
        except Exception as e:
            logger.error(f"Error al generar popularidad de repositorios: {e}")
            return pd.DataFrame()
    
    def generate_geographical_activity(self) -> pd.DataFrame:
        """
        Genera actividad geográfica (inferida de zonas horarias)
        
        Returns:
            DataFrame con actividad por región geográfica
        """
        # Nota: En un caso real, esta función inferiría ubicaciones de zonas horarias,
        # pero para el propósito de este ejemplo, generamos datos simulados
        if self.silver_cache["events"] is None:
            logger.error("No hay datos de eventos disponibles")
            return pd.DataFrame()
        
        try:
            # Obtener eventos con componentes de fecha
            events_df = self.extract_date_components()
            if events_df.empty:
                return pd.DataFrame()
            
            # Agrupar por fecha
            grouped = events_df.groupby('date')
            
            # Esta es una simplificación para el propósito del ejemplo
            # En un caso real, se inferiría la región a partir de datos reales
            regions = ['NA', 'EU', 'APAC', 'LATAM', 'OTHER']
            countries = ['US', 'GB', 'DE', 'JP', 'BR', 'IN', 'AU', 'CA', 'FR', 'ES']
            
            geo_activity = []
            current_time = datetime.datetime.now().isoformat()
            
            for date, group in tqdm(grouped, desc="Generando actividad geográfica"):
                # Simular distribución según hora del día
                for region in regions:
                    for country in countries[:2]:  # Simplificar para demo
                        # En un caso real, estos datos provendrían de inferencia de zona horaria
                        # o de datos reales de ubicación en los eventos
                        region_code = region
                        country_code = country
                        
                        # Valores simulados basados en distribución horaria
                        event_count = np.random.randint(10, 1000)
                        actor_count = np.random.randint(5, 200)
                        repo_count = np.random.randint(2, 100)
                        
                        # Tipos de eventos más comunes en esta región
                        event_types = {
                            'PushEvent': np.random.randint(5, 500),
                            'IssueCommentEvent': np.random.randint(5, 200),
                            'WatchEvent': np.random.randint(5, 100)
                        }
                        
                        geo_activity.append({
                            'date': str(date),
                            'region_code': region_code,
                            'country_code': country_code,
                            'event_count': event_count,
                            'actor_count': actor_count,
                            'repo_count': repo_count,
                            'top_event_types': json.dumps(event_types),
                            'processed_at': current_time
                        })
            
            return pd.DataFrame(geo_activity)
        
        except Exception as e:
            logger.error(f"Error al generar actividad geográfica: {e}")
            return pd.DataFrame()
    
    def save_gold_data(self, table_name: str, df: pd.DataFrame, date_str: str = None) -> Optional[Path]:
        """
        Guarda datos de capa Gold en archivo
        
        Args:
            table_name: Nombre de la tabla Gold
            df: DataFrame con datos
            date_str: Cadena de fecha opcional para el nombre del archivo
            
        Returns:
            Ruta al archivo guardado o None si falla
        """
        if df.empty:
            logger.warning(f"No hay datos para guardar en tabla {table_name}")
            return None
        
        try:
            table_dir = self.gold_dir / table_name
            table_dir.mkdir(parents=True, exist_ok=True)
            
            # Usar fecha actual si no se proporciona
            if date_str is None:
                date_str = datetime.datetime.now().strftime("%Y-%m-%d")
            
            # Guardar archivo
            file_name = f"{date_str}.{table_name}"
            if not self.use_csv:
                try:
                    output_path = table_dir / f"{file_name}.parquet"
                    df.to_parquet(output_path, index=False)
                    return output_path
                except ImportError as e:
                    logger.warning(f"Error al guardar como parquet: {e}")
                    logger.warning("Cambiando a formato CSV")
                    self.use_csv = True
            
            if self.use_csv:
                output_path = table_dir / f"{file_name}.csv"
                df.to_csv(output_path, index=False)
                return output_path
            
        except Exception as e:
            logger.error(f"Error al guardar tabla Gold {table_name}: {e}")
            return None
    
    def run(self) -> Dict[str, Any]:
        """
        Ejecuta el procesamiento Gold completo
        
        Returns:
            Diccionario con estadísticas del procesamiento
        """
        start_time = time.time()
        logger.info("Iniciando procesamiento de capa Gold")
        
        # Cargar datos Silver
        if not self.load_silver_data():
            logger.error("No se pudieron cargar datos Silver, abortando")
            return {
                "status": "error",
                "error": "No se pudieron cargar datos Silver",
                "tables_processed": 0,
                "duration_seconds": time.time() - start_time
            }
        
        # Generar cada tabla Gold
        gold_tables = {}
        table_generators = {
            "event_metrics": self.generate_event_metrics,
            "actor_metrics": self.generate_actor_metrics,
            "repo_metrics": self.generate_repo_metrics,
            "language_trends": self.generate_language_trends,
            "time_based_activity": self.generate_time_based_activity,
            "repository_popularity": self.generate_repository_popularity,
            "geographical_activity": self.generate_geographical_activity
        }
        
        # Obtener fecha para nombres de archivos
        date_str = datetime.datetime.now().strftime("%Y-%m-%d")
        
        # Generar y guardar cada tabla
        tables_results = {}
        
        for table_name, generator_func in table_generators.items():
            try:
                logger.info(f"Generando tabla Gold: {table_name}")
                df = generator_func()
                
                if df.empty:
                    logger.warning(f"No se generaron datos para tabla {table_name}")
                    tables_results[table_name] = {
                        "status": "warning",
                        "rows": 0,
                        "file": None
                    }
                    continue
                
                # Guardar tabla
                output_path = self.save_gold_data(table_name, df, date_str)
                if output_path:
                    tables_results[table_name] = {
                        "status": "success",
                        "rows": len(df),
                        "file": str(output_path)
                    }
                else:
                    tables_results[table_name] = {
                        "status": "error",
                        "rows": len(df),
                        "file": None
                    }
            
            except Exception as e:
                logger.error(f"Error al procesar tabla {table_name}: {e}")
                tables_results[table_name] = {
                    "status": "error",
                    "error": str(e),
                    "rows": 0,
                    "file": None
                }
        
        # Calcular estadísticas finales
        successful = sum(1 for result in tables_results.values() if result["status"] == "success")
        
        duration = time.time() - start_time
        logger.info(f"Procesamiento Gold completado en {duration:.2f} segundos")
        
        return {
            "status": "success" if successful > 0 else "warning",
            "tables_processed": successful,
            "tables_results": tables_results,
            "duration_seconds": duration
        }


def parse_arguments():
    """
    Parsea los argumentos de línea de comandos
    
    Returns:
        Argumentos parseados
    """
    parser = argparse.ArgumentParser(description="Procesador de capa Gold para datos de GitHub Archive")
    
    # Argumentos para procesamiento
    parser.add_argument(
        "--date",
        type=str,
        help="Fecha específica a procesar (ej: 2025-05-01)"
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
        processor = GoldProcessor(
            date_pattern=args.date,
            use_csv=args.use_csv
        )
        
        # Ejecutar procesamiento
        stats = processor.run()
        
        # Mostrar resultados
        if stats["status"] == "success":
            print(f"\n✅ Procesamiento Gold completado:")
            print(f"📊 {stats['tables_processed']}/{len(GOLD_TABLES)} tablas procesadas correctamente")
        else:
            print(f"\n⚠️ Procesamiento Gold completado con advertencias:")
            print(f"📊 {stats['tables_processed']}/{len(GOLD_TABLES)} tablas procesadas correctamente")
            if "error" in stats:
                print(f"❌ Error: {stats['error']}")
        
        # Mostrar detalles por tabla
        print("\nDetalles por tabla:")
        for table_name, result in stats["tables_results"].items():
            status_icon = "✅" if result["status"] == "success" else "⚠️" if result["status"] == "warning" else "❌"
            print(f"{status_icon} {table_name}: {result['rows']} filas")
            
        # Mostrar duración
        minutes, seconds = divmod(stats["duration_seconds"], 60)
        print(f"\n⏱️ Duración: {int(minutes)} minutos y {seconds:.2f} segundos")
        
        return 0 if stats["status"] == "success" else 1
        
    except Exception as e:
        logger.critical(f"Error en el procesamiento Gold: {e}", exc_info=True)
        print(f"\n❌ Error en el procesamiento Gold: {e}")
        return 2


if __name__ == "__main__":
    sys.exit(main())