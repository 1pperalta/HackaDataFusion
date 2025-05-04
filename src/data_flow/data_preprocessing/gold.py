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

# Directorios para datos
SILVER_DIR = Path("data/processed/silver")
GOLD_DIR   = Path("data/processed/gold")

class GoldProcessor:
    """Procesador de capa Gold para datos de GitHub Archive"""

    def __init__(self, use_csv: bool = True):
        self.use_csv = use_csv
        # Asegurar que existan directorios Gold
        GOLD_DIR.mkdir(parents=True, exist_ok=True)
        for tbl in ["actor_metrics","repo_metrics","org_metrics",
                    "event_type_metrics","daily_summary"]:
            (GOLD_DIR/tbl).mkdir(parents=True, exist_ok=True)

    def read_silver_table(self, table_name: str) -> pd.DataFrame:
        pattern_csv     = str(SILVER_DIR/table_name/"*.csv")
        pattern_parquet = str(SILVER_DIR/table_name/"*.parquet")
        files = glob.glob(pattern_parquet) + glob.glob(pattern_csv)
        if not files:
            logger.warning(f"[WARN] No files for silver/{table_name}")
            return pd.DataFrame()
        dfs = []
        for f in files:
            try:
                dfs.append(pd.read_parquet(f) if f.endswith(".parquet") else pd.read_csv(f))
            except Exception as e:
                logger.error(f"[ERROR] reading {f}: {e}")
        df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        if df.empty:
            logger.warning(f"[WARN] silver/{table_name} loaded 0 rows")
        return df

    def process_actor_metrics(self, events: pd.DataFrame, actors: pd.DataFrame) -> pd.DataFrame:
        if events.empty:
            logger.warning("[WARN] 'events' empty; skipping actor_metrics")
            return pd.DataFrame()
        m = (events.groupby("actor_id")
             .agg(total_events=("event_id","count"),
                  unique_repos=("repo_id",pd.Series.nunique),
                  first_event=("created_at","min"),
                  last_event=("created_at","max"))
             .reset_index())
        if not actors.empty:
            if 'actor_id' in actors.columns:
                dedup = actors.drop_duplicates(subset=['actor_id'])
                m = m.merge(dedup, on='actor_id', how='left')
            else:
                logger.warning("[WARN] 'actors' missing 'actor_id'; skipping actor join")
        return m

    def process_repo_metrics(self, events: pd.DataFrame, repos: pd.DataFrame) -> pd.DataFrame:
        if events.empty:
            logger.warning("[WARN] 'events' empty; skipping repo_metrics")
            return pd.DataFrame()
        m = (events.groupby("repo_id")
             .agg(total_events=("event_id","count"),
                  unique_actors=("actor_id",pd.Series.nunique),
                  first_event=("created_at","min"),
                  last_event=("created_at","max"))
             .reset_index())
        if not repos.empty:
            if 'repo_id' in repos.columns:
                dedup = repos.drop_duplicates(subset=['repo_id'])
                m = m.merge(dedup, on='repo_id', how='left')
            else:
                logger.warning("[WARN] 'repositories' missing 'repo_id'; skipping repo join")
        return m

    def process_org_metrics(self, events: pd.DataFrame, orgs: pd.DataFrame) -> pd.DataFrame:
        if events.empty:
            logger.warning("[WARN] 'events' empty; skipping org_metrics")
            return pd.DataFrame()
        df = events.dropna(subset=['org_id'])
        if df.empty:
            logger.warning("[WARN] no events with 'org_id'; skipping org_metrics")
            return pd.DataFrame()
        m = (df.groupby("org_id")
             .agg(total_events=("event_id","count"),
                  unique_actors=("actor_id",pd.Series.nunique),
                  first_event=("created_at","min"),
                  last_event=("created_at","max"))
             .reset_index())
        if not orgs.empty:
            if 'org_id' in orgs.columns:
                dedup = orgs.drop_duplicates(subset=['org_id'])
                m = m.merge(dedup, on='org_id', how='left')
            else:
                logger.warning("[WARN] 'organizations' missing 'org_id'; skipping org join")
        return m

    def process_event_type_metrics(self, events: pd.DataFrame) -> pd.DataFrame:
        if events.empty:
            logger.warning("[WARN] 'events' empty; skipping event_type_metrics")
            return pd.DataFrame()
        return events.groupby('event_type').size().reset_index(name='count')

    def process_daily_summary(self, events: pd.DataFrame) -> pd.DataFrame:
        if events.empty:
            logger.warning("[WARN] 'events' empty; skipping daily_summary")
            return pd.DataFrame()
        return (events.groupby('hour_bucket')
                .agg(total_events=("event_id","count"),
                     unique_actors=("actor_id",pd.Series.nunique),
                     unique_repos=("repo_id",pd.Series.nunique))
                .reset_index())

    def save_gold_data(self, name: str, df: pd.DataFrame) -> None:
        if df.empty:
            logger.warning(f"[WARN] gold/{name} empty; not saving")
            return
        out_dir = GOLD_DIR/name
        date_str = datetime.datetime.now().strftime("%Y-%m-%d")
        base = f"{date_str}.{name}"
        # Try Parquet, fallback CSV
        try:
            if not self.use_csv:
                path = out_dir/f"{base}.parquet"
                df.to_parquet(path, index=False)
                logger.info(f"Guardado {path}")
                return
        except Exception:
            self.use_csv = True
        path = out_dir/f"{base}.csv"
        df.to_csv(path, index=False)
        logger.info(f"Guardado {path}")

    def run(self) -> Dict[str,int]:
        start = datetime.datetime.now()
        logger.info("Leyendo datos Silver...")
        events = self.read_silver_table('events')
        actors  = self.read_silver_table('actors')
        repos   = self.read_silver_table('repositories')
        orgs    = self.read_silver_table('organizations')

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
    return parser.parse_args()


def main():
    Path("logs").mkdir(exist_ok=True)
    args = parse_args()
    processor = GoldProcessor(use_csv=args.use_csv)
    stats = processor.run()

    print("\n✅ Procesamiento Gold completado:")
    for tbl, cnt in stats.items():
        mark = "✅" if cnt>0 else "⚠️"
        print(f"{mark} {tbl}: {cnt} filas")

if __name__ == "__main__":
    main()
