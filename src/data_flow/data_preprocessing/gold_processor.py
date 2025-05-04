#!/usr/bin/env python
"""
GitHub Archive Gold Processor

Este script procesa los datos de la capa Silver de GitHub Archive 
para generar la capa Gold, con agregaciones y métricas finales 
listas para reporting y análisis.

La capa Gold contiene tablas con métricas por actor, repositorio,
organización y tipos de evento, así como resúmenes diarios.
"""

import sys
import logging
import argparse
import datetime
import glob
import os
from pathlib import Path
from typing import Dict, Any

import pandas as pd
from tqdm import tqdm

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/gold_processor.log", mode='a')
    ]
)
logger = logging.getLogger("gold_processor")

# Determinar la ruta del proyecto
# Asumimos que este script está en src/data_flow/data_preprocessing/gold.py
SCRIPT_PATH = Path(os.path.abspath(__file__))
PROJECT_ROOT = SCRIPT_PATH.parent.parent.parent.parent  # Subir 3 niveles desde el script
logger.info(f"Raíz del proyecto: {PROJECT_ROOT}")

# Directorios para datos
DATA_DIR = PROJECT_ROOT / "data"
PROCESSED_DIR = DATA_DIR / "processed"
SILVER_DIR = PROCESSED_DIR / "silver"
GOLD_DIR = PROCESSED_DIR / "gold"

class GoldProcessor:
    """Procesador de capa Gold para datos de GitHub Archive"""

    def __init__(self, use_csv: bool = True):
        self.use_csv = use_csv
        # Asegurar que existan directorios Gold
        GOLD_DIR.mkdir(parents=True, exist_ok=True)
        for tbl in ["actor_metrics", "repo_metrics", "org_metrics",
                    "event_type_metrics", "daily_summary"]:
            (GOLD_DIR / tbl).mkdir(parents=True, exist_ok=True)

    def read_silver_table(self, table_name: str) -> pd.DataFrame:
        """
        Lee datos de tablas Silver usando patrones de nombre de archivo que coincidan con la estructura real
        """
        # Verificar si existe el directorio Silver
        if not SILVER_DIR.exists():
            logger.error(f"Directorio Silver no existe: {SILVER_DIR}")
            
            # Buscar posibles ubicaciones alternativas
            alt_dirs = [
                PROJECT_ROOT / "data" / "silver",
                PROJECT_ROOT / "data" / "processed" / "silver",
                PROJECT_ROOT / "dbt" / "github_analytics" / "data" / "silver",
                PROJECT_ROOT / "src" / "data" / "silver"
            ]
            
            for alt_dir in alt_dirs:
                if alt_dir.exists():
                    logger.info(f"Encontrado directorio alternativo: {alt_dir}")
                    return self.read_files_from_location(alt_dir, table_name)
            
            return pd.DataFrame()
        
        # Buscar en ubicación principal
        return self.read_files_from_location(SILVER_DIR, table_name)
    
    def read_files_from_location(self, base_dir: Path, table_name: str) -> pd.DataFrame:
        """
        Lee archivos desde una ubicación base específica
        """
        # Diferentes patrones a probar
        patterns = [
            # Patrón según la captura de pantalla
            str(base_dir / table_name / f"github_events_*.{table_name}.parquet"),
            # Patrones alternativos
            str(base_dir / table_name / f"*.{table_name}.parquet"),
            str(base_dir / table_name / "*.parquet"),
            str(base_dir / f"{table_name}" / "*.parquet"),
            str(base_dir / f"{table_name}.parquet"),
            str(base_dir / f"silver_{table_name}.parquet")
        ]
        
        # Buscar en todos los patrones
        all_files = []
        for pattern in patterns:
            logger.info(f"Buscando archivos con patrón: {pattern}")
            files = glob.glob(pattern)
            if files:
                logger.info(f"Encontrados {len(files)} archivos con patrón {pattern}")
                all_files.extend(files)
        
        if not all_files:
            logger.warning(f"[WARN] No files found for silver/{table_name} after trying multiple patterns")
            return pd.DataFrame()
        
        # Eliminar duplicados
        all_files = list(set(all_files))
        logger.info(f"Encontrados {len(all_files)} archivos únicos para {table_name}")
        
        # Leer archivos
        dfs = []
        for f in all_files:
            try:
                logger.info(f"Leyendo archivo: {f}")
                df = pd.read_parquet(f)
                logger.info(f"Archivo leído correctamente: {len(df)} filas")
                dfs.append(df)
            except Exception as e:
                logger.error(f"[ERROR] reading {f}: {e}")
        
        if not dfs:
            logger.warning(f"[WARN] No se pudieron leer datos para {table_name}")
            return pd.DataFrame()
        
        # Combinar dataframes
        df = pd.concat(dfs, ignore_index=True)
        logger.info(f"Tabla {table_name} cargada con {len(df)} filas y columnas: {df.columns.tolist()}")
        return df

    def process_actor_metrics(self, events: pd.DataFrame, actors: pd.DataFrame) -> pd.DataFrame:
        if events.empty:
            logger.warning("[WARN] 'events' empty; skipping actor_metrics")
            return pd.DataFrame()
        
        logger.info(f"Procesando {len(events)} eventos para métricas de actores")
        
        # Asegurar que las columnas necesarias estén presentes
        required_cols = ["actor_id", "event_id", "repo_id", "created_at"]
        missing_cols = [col for col in required_cols if col not in events.columns]
        
        if missing_cols:
            logger.warning(f"[WARN] Columnas faltantes en events: {missing_cols}")
            # Intentar mapear a nombres alternativos si es necesario
            col_map = {
                "actor_id": ["actor_id", "actorid", "actor"],
                "event_id": ["event_id", "id", "eventid"],
                "repo_id": ["repo_id", "repoid", "repository_id"],
                "created_at": ["created_at", "createdat", "date", "timestamp"]
            }
            
            for col in missing_cols:
                for alt in col_map.get(col, []):
                    if alt in events.columns:
                        events[col] = events[alt]
                        logger.info(f"Usando columna alternativa {alt} para {col}")
                        break
        
        # Verificar de nuevo si faltan columnas después del mapeo
        missing_cols = [col for col in required_cols if col not in events.columns]
        if missing_cols:
            logger.error(f"[ERROR] No se pueden procesar métricas de actores: faltan columnas {missing_cols}")
            return pd.DataFrame()
        
        m = (events.groupby("actor_id")
             .agg(total_events=("event_id", "count"),
                  unique_repos=("repo_id", pd.Series.nunique),
                  first_event=("created_at", "min"),
                  last_event=("created_at", "max"))
             .reset_index())
        
        if not actors.empty:
            if 'actor_id' in actors.columns:
                logger.info(f"Uniendo con tabla de actores ({len(actors)} filas)")
                dedup = actors.drop_duplicates(subset=['actor_id'])
                m = m.merge(dedup, on='actor_id', how='left')
            else:
                logger.warning("[WARN] 'actors' missing 'actor_id'; skipping actor join")
        
        return m

    def process_repo_metrics(self, events: pd.DataFrame, repos: pd.DataFrame) -> pd.DataFrame:
        if events.empty:
            logger.warning("[WARN] 'events' empty; skipping repo_metrics")
            return pd.DataFrame()
        
        logger.info(f"Procesando {len(events)} eventos para métricas de repositorios")
        
        # Asegurar que las columnas necesarias estén presentes
        required_cols = ["repo_id", "event_id", "actor_id", "created_at"]
        missing_cols = [col for col in required_cols if col not in events.columns]
        
        if missing_cols:
            logger.warning(f"[WARN] Columnas faltantes en events: {missing_cols}")
            # Intentar mapear a nombres alternativos si es necesario
            col_map = {
                "repo_id": ["repo_id", "repoid", "repository_id"],
                "event_id": ["event_id", "id", "eventid"],
                "actor_id": ["actor_id", "actorid", "actor"],
                "created_at": ["created_at", "createdat", "date", "timestamp"]
            }
            
            for col in missing_cols:
                for alt in col_map.get(col, []):
                    if alt in events.columns:
                        events[col] = events[alt]
                        logger.info(f"Usando columna alternativa {alt} para {col}")
                        break
        
        # Verificar de nuevo si faltan columnas después del mapeo
        missing_cols = [col for col in required_cols if col not in events.columns]
        if missing_cols:
            logger.error(f"[ERROR] No se pueden procesar métricas de repositorios: faltan columnas {missing_cols}")
            return pd.DataFrame()
        
        m = (events.groupby("repo_id")
             .agg(total_events=("event_id", "count"),
                  unique_actors=("actor_id", pd.Series.nunique),
                  first_event=("created_at", "min"),
                  last_event=("created_at", "max"))
             .reset_index())
        
        if not repos.empty:
            if 'repo_id' in repos.columns:
                logger.info(f"Uniendo con tabla de repositorios ({len(repos)} filas)")
                dedup = repos.drop_duplicates(subset=['repo_id'])
                m = m.merge(dedup, on='repo_id', how='left')
            else:
                logger.warning("[WARN] 'repositories' missing 'repo_id'; skipping repo join")
        
        return m

    def process_org_metrics(self, events: pd.DataFrame, orgs: pd.DataFrame) -> pd.DataFrame:
        if events.empty:
            logger.warning("[WARN] 'events' empty; skipping org_metrics")
            return pd.DataFrame()
        
        # Verificar si existe la columna org_id
        if 'org_id' not in events.columns:
            # Intentar usar una columna alternativa
            for alt in ['orgid', 'organization_id', 'org']:
                if alt in events.columns:
                    events['org_id'] = events[alt]
                    logger.info(f"Usando columna alternativa {alt} para org_id")
                    break
            else:
                logger.warning("[WARN] No se encontró columna 'org_id' o alternativa")
                return pd.DataFrame()
        
        df = events.dropna(subset=['org_id'])
        if df.empty:
            logger.warning("[WARN] no events with 'org_id'; skipping org_metrics")
            return pd.DataFrame()
        
        logger.info(f"Procesando {len(df)} eventos con org_id para métricas de organizaciones")
        
        m = (df.groupby("org_id")
             .agg(total_events=("event_id", "count"),
                  unique_actors=("actor_id", pd.Series.nunique),
                  first_event=("created_at", "min"),
                  last_event=("created_at", "max"))
             .reset_index())
        
        if not orgs.empty:
            if 'org_id' in orgs.columns:
                logger.info(f"Uniendo con tabla de organizaciones ({len(orgs)} filas)")
                dedup = orgs.drop_duplicates(subset=['org_id'])
                m = m.merge(dedup, on='org_id', how='left')
            else:
                logger.warning("[WARN] 'organizations' missing 'org_id'; skipping org join")
        
        return m

    def process_event_type_metrics(self, events: pd.DataFrame) -> pd.DataFrame:
        if events.empty:
            logger.warning("[WARN] 'events' empty; skipping event_type_metrics")
            return pd.DataFrame()
        
        # Verificar si existe la columna event_type
        if 'event_type' not in events.columns:
            # Intentar usar una columna alternativa
            for alt in ['type', 'eventtype', 'event']:
                if alt in events.columns:
                    events['event_type'] = events[alt]
                    logger.info(f"Usando columna alternativa {alt} para event_type")
                    break
            else:
                logger.warning("[WARN] No se encontró columna 'event_type' o alternativa")
                return pd.DataFrame()
        
        logger.info(f"Procesando {len(events)} eventos para métricas de tipos de evento")
        return events.groupby('event_type').size().reset_index(name='count')

    def process_daily_summary(self, events: pd.DataFrame) -> pd.DataFrame:
        if events.empty:
            logger.warning("[WARN] 'events' empty; skipping daily_summary")
            return pd.DataFrame()
        
        # Verificar si existe la columna hour_bucket o crear una a partir de created_at
        if 'hour_bucket' not in events.columns:
            if 'created_at' in events.columns:
                # Convertir a datetime si es string
                if events['created_at'].dtype == 'object':
                    try:
                        events['created_at'] = pd.to_datetime(events['created_at'])
                    except Exception as e:
                        logger.error(f"[ERROR] No se pudo convertir created_at a datetime: {e}")
                        return pd.DataFrame()
                
                # Crear hour_bucket
                events['hour_bucket'] = events['created_at'].dt.floor('H')
                logger.info("Creada columna hour_bucket a partir de created_at")
            else:
                logger.warning("[WARN] No se encontró columna 'hour_bucket' o 'created_at'")
                return pd.DataFrame()
        
        logger.info(f"Procesando {len(events)} eventos para resumen diario")
        return (events.groupby('hour_bucket')
                .agg(total_events=("event_id", "count"),
                     unique_actors=("actor_id", pd.Series.nunique),
                     unique_repos=("repo_id", pd.Series.nunique))
                .reset_index())

    def save_gold_data(self, name: str, df: pd.DataFrame) -> None:
        if df.empty:
            logger.warning(f"[WARN] gold/{name} empty; not saving")
            return
        
        out_dir = GOLD_DIR / name
        out_dir.mkdir(parents=True, exist_ok=True)
        
        date_str = datetime.datetime.now().strftime("%Y-%m-%d")
        base = f"{date_str}.{name}"
        
        # Try Parquet, fallback CSV
        try:
            if not self.use_csv:
                path = out_dir / f"{base}.parquet"
                df.to_parquet(path, index=False)
                logger.info(f"Guardado {path}")
                return
        except Exception as e:
            logger.warning(f"Error guardando parquet: {e}")
            self.use_csv = True
            
        path = out_dir / f"{base}.csv"
        df.to_csv(path, index=False)
        logger.info(f"Guardado {path}")

    def run(self) -> Dict[str, int]:
        start = datetime.datetime.now()
        logger.info("Leyendo datos Silver...")
        events = self.read_silver_table('events')
        actors = self.read_silver_table('actors')
        repos = self.read_silver_table('repositories')
        orgs = self.read_silver_table('organizations')

        stats = {}
        logger.info("Procesando actor_metrics...")
        am = self.process_actor_metrics(events, actors)
        self.save_gold_data('actor_metrics', am)
        stats['actor_metrics'] = len(am)

        logger.info("Procesando repo_metrics...")
        rm = self.process_repo_metrics(events, repos)
        self.save_gold_data('repo_metrics', rm)
        stats['repo_metrics'] = len(rm)

        logger.info("Procesando org_metrics...")
        om = self.process_org_metrics(events, orgs)
        self.save_gold_data('org_metrics', om)
        stats['org_metrics'] = len(om)

        logger.info("Procesando event_type_metrics...")
        etm = self.process_event_type_metrics(events)
        self.save_gold_data('event_type_metrics', etm)
        stats['event_type_metrics'] = len(etm)

        logger.info("Procesando daily_summary...")
        ds = self.process_daily_summary(events)
        self.save_gold_data('daily_summary', ds)
        stats['daily_summary'] = len(ds)

        duration = (datetime.datetime.now() - start).total_seconds()
        logger.info(f"Procesamiento Gold completado en {duration:.2f} segundos")
        return stats


def parse_args():
    parser = argparse.ArgumentParser(description="Procesador de capa Gold")
    parser.add_argument("--use-csv", action="store_true",
                       help="Forzar CSV en lugar de Parquet")
    parser.add_argument("--debug", action="store_true",
                       help="Habilitar modo debug con más logging")
    parser.add_argument("--data-dir", type=str, default=None,
                       help="Directorio explícito donde buscar datos")
    return parser.parse_args()


def main():
    Path("logs").mkdir(exist_ok=True)
    args = parse_args()
    
    # Configurar nivel de logging
    if args.debug:
        logging.getLogger("gold_processor").setLevel(logging.DEBUG)
        logger.debug("Modo debug activado")
    
    # Sobrescribir directorio de datos si se especifica
    global DATA_DIR, PROCESSED_DIR, SILVER_DIR, GOLD_DIR
    if args.data_dir:
        DATA_DIR = Path(args.data_dir)
        PROCESSED_DIR = DATA_DIR / "processed"
        SILVER_DIR = PROCESSED_DIR / "silver"
        GOLD_DIR = PROCESSED_DIR / "gold"
    
    # Listar directorios en la raíz del proyecto para depuración
    logger.info("Listando directorios en la raíz del proyecto:")
    for item in PROJECT_ROOT.iterdir():
        if item.is_dir():
            logger.info(f"  - {item}")
    
    # Mostrar información sobre directorios
    logger.info(f"Directorio de datos: {DATA_DIR.absolute()}")
    logger.info(f"Directorio Silver: {SILVER_DIR.absolute()}")
    logger.info(f"Directorio Gold: {GOLD_DIR.absolute()}")
    
    processor = GoldProcessor(use_csv=args.use_csv)
    stats = processor.run()

    print("\n✅ Procesamiento Gold completado:")
    for tbl, cnt in stats.items():
        mark = "✅" if cnt > 0 else "⚠️"
        print(f"{mark} {tbl}: {cnt} filas")


if __name__ == "__main__":
    main()