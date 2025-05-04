#!/usr/bin/env python
"""
GitHub Archive Pipeline Diagnostics Tool

This script diagnoses potential issues in the medallion data pipeline by:
1. Analyzing data availability in each layer (bronze, silver, gold)
2. Checking data completeness and schema integrity
3. Identifying where data might be getting lost between layers
4. Providing recommendations for fixes

Use this tool when tables in the gold layer are empty or have unexpected results.
"""

import os
import sys
import logging
import argparse
import datetime
import json
import glob
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import time

import pandas as pd
import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/pipeline_diagnostics.log", mode='a')
    ]
)
logger = logging.getLogger("pipeline_diagnostics")

# Data directories
RAW_DIR = Path("data/raw")
BRONZE_DIR = Path("data/processed/bronze")
SILVER_DIR = Path("data/processed/silver")
GOLD_DIR = Path("data/processed/gold")

class PipelineDiagnostics:
    """Diagnoses issues in the medallion data pipeline"""
    
    def __init__(
        self,
        raw_dir: Path = RAW_DIR,
        bronze_dir: Path = BRONZE_DIR,
        silver_dir: Path = SILVER_DIR,
        gold_dir: Path = GOLD_DIR,
        verbose: bool = False
    ):
        """
        Initialize the diagnostics tool
        
        Args:
            raw_dir: Directory with raw data
            bronze_dir: Directory with bronze data
            silver_dir: Directory with silver data
            gold_dir: Directory with gold data
            verbose: Whether to show more detailed output
        """
        self.raw_dir = raw_dir
        self.bronze_dir = bronze_dir
        self.silver_dir = silver_dir
        self.gold_dir = gold_dir
        self.verbose = verbose
        
        # Create logs directory if it doesn't exist
        Path("logs").mkdir(exist_ok=True)
    
    def find_files_by_layer(self, layer: str, pattern: str = "*") -> Dict[str, List[Path]]:
        """
        Find files in a specific layer
        
        Args:
            layer: Layer name ('raw', 'bronze', 'silver', 'gold')
            pattern: Optional glob pattern
            
        Returns:
            Dictionary mapping types to file paths
        """
        if layer == "raw":
            dir_path = self.raw_dir
            search_pattern = f"**/*.json.gz"
            files = list(dir_path.glob(search_pattern))
            return {"raw_files": files}
            
        elif layer == "bronze":
            dir_path = self.bronze_dir
            search_patterns = ["**/*.parquet", "**/*.csv"]
            all_files = []
            for pattern in search_patterns:
                all_files.extend(list(dir_path.glob(pattern)))
            return {"bronze_files": all_files}
            
        elif layer == "silver":
            dir_path = self.silver_dir
            results = {}
            
            # Silver has subdirectories for different table types
            for subdir in ["events", "actors", "repositories", "organizations", "payload_details"]:
                subdir_path = dir_path / subdir
                if subdir_path.exists():
                    search_patterns = ["*.parquet", "*.csv"]
                    table_files = []
                    for pattern in search_patterns:
                        table_files.extend(list(subdir_path.glob(pattern)))
                    results[subdir] = table_files
            
            return results
            
        elif layer == "gold":
            dir_path = self.gold_dir
            results = {}
            
            # Gold has subdirectories for different table types
            for subdir in dir_path.iterdir():
                if subdir.is_dir():
                    search_patterns = ["*.parquet", "*.csv"]
                    table_files = []
                    for pattern in search_patterns:
                        table_files.extend(list(subdir.glob(pattern)))
                    results[subdir.name] = table_files
            
            return results
        
        else:
            logger.error(f"Unknown layer: {layer}")
            return {}
    
    def count_files_by_layer(self) -> Dict[str, Dict[str, int]]:
        """
        Count files in each layer
        
        Returns:
            Dictionary with counts by layer and type
        """
        results = {}
        
        # Count raw files
        raw_files = self.find_files_by_layer("raw")
        raw_count = sum(len(files) for files in raw_files.values())
        results["raw"] = {"total": raw_count}
        
        # Count bronze files
        bronze_files = self.find_files_by_layer("bronze")
        bronze_count = sum(len(files) for files in bronze_files.values())
        results["bronze"] = {"total": bronze_count}
        
        # Count silver files by type
        silver_files = self.find_files_by_layer("silver")
        silver_counts = {table: len(files) for table, files in silver_files.items()}
        silver_counts["total"] = sum(silver_counts.values())
        results["silver"] = silver_counts
        
        # Count gold files by type
        gold_files = self.find_files_by_layer("gold")
        gold_counts = {table: len(files) for table, files in gold_files.items()}
        gold_counts["total"] = sum(gold_counts.values())
        results["gold"] = gold_counts
        
        return results
    
    def sample_file_content(self, file_path: Path, sample_rows: int = 5) -> Optional[pd.DataFrame]:
        """
        Sample content from a file
        
        Args:
            file_path: Path to the file
            sample_rows: Number of rows to sample
            
        Returns:
            DataFrame with sample content or None if error
        """
        try:
            if file_path.suffix == ".parquet":
                return pd.read_parquet(file_path).head(sample_rows)
            elif file_path.suffix == ".csv":
                return pd.read_csv(file_path).head(sample_rows)
            elif file_path.suffix == ".gz" and file_path.name.endswith(".json.gz"):
                import gzip
                import json
                
                rows = []
                with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                    for i, line in enumerate(f):
                        if i >= sample_rows:
                            break
                        try:
                            rows.append(json.loads(line.strip()))
                        except json.JSONDecodeError:
                            continue
                
                return pd.DataFrame(rows) if rows else None
            else:
                logger.warning(f"Unsupported file format: {file_path}")
                return None
        except Exception as e:
            logger.error(f"Error sampling file {file_path}: {e}")
            return None
    
    def analyze_table_schema(self, layer: str, table_type: str) -> Dict[str, Any]:
        """
        Analyze the schema of a table type in a specific layer
        
        Args:
            layer: Layer name ('bronze', 'silver', 'gold')
            table_type: Table type or subdirectory name
            
        Returns:
            Dictionary with schema information
        """
        files_by_layer = self.find_files_by_layer(layer)
        
        if table_type not in files_by_layer:
            return {"error": f"Table type {table_type} not found in {layer} layer"}
        
        table_files = files_by_layer[table_type]
        if not table_files:
            return {"error": f"No files found for {table_type} in {layer} layer"}
        
        # Sample the first file
        sample_file = table_files[0]
        sample_df = self.sample_file_content(sample_file)
        
        if sample_df is None:
            return {"error": f"Could not read {sample_file}"}
        
        # Analyze schema
        columns = list(sample_df.columns)
        dtypes = {str(col): str(dtype) for col, dtype in sample_df.dtypes.items()}
        non_null_counts = {col: sample_df[col].count() for col in columns}
        
        return {
            "file": str(sample_file),
            "column_count": len(columns),
            "columns": columns,
            "dtypes": dtypes,
            "non_null_counts": non_null_counts,
            "row_count": len(sample_df)
        }
    
    def check_data_consistency(self) -> Dict[str, Any]:
        """
        Check for data consistency across layers
        
        Returns:
            Dictionary with consistency check results
        """
        results = {
            "bronze_to_silver": {},
            "silver_to_gold": {},
            "missing_required_columns": {},
            "recommendations": [],
            "column_details": {}  # Add details about specific columns
        }
        
        # Check silver tables for required event columns
        silver_files = self.find_files_by_layer("silver")
        
        # Check events table
        if "events" in silver_files and silver_files["events"]:
            events_sample = self.sample_file_content(silver_files["events"][0])
            if events_sample is not None:
                # Check essential columns
                required_cols = ["event_id", "event_type", "created_at", "actor_id", "repo_id"]
                missing_cols = [col for col in required_cols if col not in events_sample.columns]
                
                if missing_cols:
                    results["missing_required_columns"]["silver_events"] = missing_cols
                    results["recommendations"].append(
                        f"Silver events table is missing key columns: {', '.join(missing_cols)}. "
                        f"These are needed for gold tables like event_metrics."
                    )
        else:
            results["recommendations"].append(
                "No silver events table found. This is essential for most gold tables."
            )
        
        # Check actors table
        if "actors" in silver_files and silver_files["actors"]:
            actors_sample = self.sample_file_content(silver_files["actors"][0])
            if actors_sample is not None:
                # Check if there's an actor_type column
                if "actor_type" not in actors_sample.columns:
                    results["recommendations"].append(
                        "Silver actors table is missing 'actor_type' column. "
                        "This may be why actor_metrics table is empty."
                    )
                else:
                    # Get details about actor_type column values
                    if len(actors_sample) > 0:
                        results["column_details"]["actor_type"] = actors_sample["actor_type"].value_counts().to_dict()
                    else:
                        results["recommendations"].append(
                            "Silver actors table has actor_type column but no data."
                        )
        else:
            results["recommendations"].append(
                "No silver actors table found. This is needed for actor_metrics gold table."
            )
        
        # Check repositories table
        if "repositories" in silver_files and silver_files["repositories"]:
            repos_sample = self.sample_file_content(silver_files["repositories"][0])
            if repos_sample is not None:
                # Check if there's a language column
                if "language" not in repos_sample.columns:
                    results["recommendations"].append(
                        "Silver repositories table is missing 'language' column. "
                        "This may be why language_trends table is empty."
                    )
                else:
                    # Get details about language column values
                    if len(repos_sample) > 0:
                        results["column_details"]["language"] = repos_sample["language"].value_counts().to_dict()
                    else:
                        results["recommendations"].append(
                            "Silver repositories table has language column but no data."
                        )
        else:
            results["recommendations"].append(
                "No silver repositories table found. This is needed for repo_metrics and language_trends gold tables."
            )
        
        # Check if geographical_activity is the only non-empty table because it uses simulated data
        gold_files = self.find_files_by_layer("gold")
        has_geo = "geographical_activity" in gold_files and gold_files["geographical_activity"]
        has_time = "time_based_activity" in gold_files and gold_files["time_based_activity"]
        
        empty_tables = 0
        for table, files in gold_files.items():
            if table not in ["geographical_activity", "time_based_activity"] and not files:
                empty_tables += 1
        
        if has_geo and empty_tables >= 3:
            results["recommendations"].append(
                "Note: The geographical_activity table uses simulated data and doesn't depend on "
                "real silver data, which explains why it's populated while others are empty."
            )
        
        return results
    
    def generate_full_report(self) -> Dict[str, Any]:
        """
        Generate a comprehensive diagnostic report
        
        Returns:
            Dictionary with complete diagnostic information
        """
        start_time = time.time()
        report = {
            "file_counts": self.count_files_by_layer(),
            "data_consistency": self.check_data_consistency(),
            "recommendations": [],
            "silver_schemas": {},
            "sample_data": {}
        }
        
        # Add silver table schemas
        silver_files = self.find_files_by_layer("silver")
        for table_type in silver_files.keys():
            if silver_files[table_type]:
                report["silver_schemas"][table_type] = self.analyze_table_schema("silver", table_type)
        
        # Generate overall recommendations
        if report["file_counts"]["silver"]["total"] == 0:
            report["recommendations"].append(
                "No silver files found. The gold layer can't process data without silver files."
            )
        
        # Check if the events table is missing or empty
        if "events" not in silver_files or not silver_files["events"]:
            report["recommendations"].append(
                "Silver events table is missing. This is critical for gold generation."
            )
        else:
            # Try to load a sample file to check if it has data
            try:
                sample_file = silver_files["events"][0]
                if sample_file.suffix == ".parquet":
                    df = pd.read_parquet(sample_file)
                else:
                    df = pd.read_csv(sample_file)
                
                if df.empty:
                    report["recommendations"].append(
                        "Silver events table exists but is empty. Check the silver processing script."
                    )
                elif len(df) < 10:  # Arbitrary small number
                    report["recommendations"].append(
                        f"Silver events table has very few rows ({len(df)}). This might not be enough for meaningful gold metrics."
                    )
            except Exception as e:
                report["recommendations"].append(
                    f"Error reading silver events file: {e}. The file might be corrupted."
                )
        
        # Add execution time
        duration = time.time() - start_time
        report["execution_time"] = duration
        
        return report
    
    def print_report(self, report: Dict[str, Any]):
        """
        Print a formatted diagnostic report
        
        Args:
            report: Report dictionary from generate_full_report
        """
        print("\n" + "="*80)
        print("📊 GITHUB ARCHIVE PIPELINE DIAGNOSTICS REPORT")
        print("="*80)
        
        # Print file counts
        print("\n📁 FILE COUNTS BY LAYER:")
        print("-" * 40)
        
        for layer, counts in report["file_counts"].items():
            print(f"  {layer.upper()} Layer:")
            for type_name, count in counts.items():
                print(f"    - {type_name}: {count} files")
        
        # Print silver schema information if verbose
        if self.verbose and "silver_schemas" in report:
            print("\n📋 SILVER TABLES SCHEMA INFORMATION:")
            print("-" * 40)
            
            for table, schema in report["silver_schemas"].items():
                if "error" in schema:
                    print(f"  {table}: {schema['error']}")
                    continue
                    
                print(f"  {table}:")
                print(f"    - File: {schema['file']}")
                print(f"    - Columns: {schema['column_count']}")
                print(f"    - Row Sample Count: {schema['row_count']}")
                if self.verbose:
                    print(f"    - Column Names: {', '.join(schema['columns'])}")
        
        # Print recommendations
        print("\n🔍 DIAGNOSTICS FINDINGS:")
        print("-" * 40)
        
        all_recommendations = report["recommendations"].copy()
        if "data_consistency" in report and "recommendations" in report["data_consistency"]:
            all_recommendations.extend(report["data_consistency"]["recommendations"])
        
        if all_recommendations:
            for i, rec in enumerate(all_recommendations, 1):
                print(f"  {i}. {rec}")
        else:
            print("  No specific issues found. If you're still experiencing problems,")
            print("  please check the log files for more detailed error messages.")
        
        # Print missing required columns if any
        if "data_consistency" in report and "missing_required_columns" in report["data_consistency"]:
            missing_cols = report["data_consistency"]["missing_required_columns"]
            if missing_cols:
                print("\n❌ MISSING REQUIRED COLUMNS:")
                print("-" * 40)
                
                for table, cols in missing_cols.items():
                    print(f"  {table}: {', '.join(cols)}")
        
        # Print summary
        print("\n📝 SUMMARY:")
        print("-" * 40)
        
        # Check if we have silver data
        has_silver = report["file_counts"]["silver"]["total"] > 0
        # Check if we have gold data
        has_gold = report["file_counts"]["gold"]["total"] > 0
        
        if not has_silver:
            print("  ❌ No silver data available. Gold processing cannot succeed without silver data.")
            print("     Check the silver processing script and logs for errors.")
        elif not has_gold:
            print("  ⚠️  Silver data exists but no gold data was generated.")
            print("     Check the gold processing script and logs for errors.")
        else:
            gold_tables = report["file_counts"]["gold"]
            empty_tables = sum(1 for table, count in gold_tables.items() 
                              if table != "total" and count == 0)
            
            if empty_tables > 0:
                print(f"  ⚠️  {empty_tables} gold tables are empty despite having silver data.")
                print("     This is likely due to missing or incorrect data in the silver layer.")
            else:
                print("  ✅ The pipeline appears to be working correctly.")
                print("     If some gold tables are empty, it may be due to specific data requirements.")
        
        # Print execution time
        minutes, seconds = divmod(report["execution_time"], 60)
        print(f"\n⏱️  Diagnostic completed in {int(minutes)} minutes and {seconds:.2f} seconds")
    
    def run(self) -> Dict[str, Any]:
        """
        Run diagnostics and print the report
        
        Returns:
            Dictionary with diagnostic results
        """
        logger.info("Starting pipeline diagnostics")
        
        try:
            # Generate report
            report = self.generate_full_report()
            
            # Print the report
            self.print_report(report)
            
            logger.info("Diagnostics completed successfully")
            return report
            
        except Exception as e:
            logger.error(f"Error running diagnostics: {e}", exc_info=True)
            print(f"\n❌ Error running diagnostics: {e}")
            return {"error": str(e)}


def parse_arguments():
    """
    Parse command line arguments
    
    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(description="Diagnose issues in the GitHub Archive pipeline")
    
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show more detailed information"
    )
    
    return parser.parse_args()


def main():
    """Main function"""
    try:
        # Create logs directory if it doesn't exist
        Path("logs").mkdir(exist_ok=True)
        
        # Parse arguments
        args = parse_arguments()
        
        # Run diagnostics
        diagnostics = PipelineDiagnostics(verbose=args.verbose)
        diagnostics.run()
        
        return 0
        
    except Exception as e:
        logger.critical(f"Error in pipeline diagnostics: {e}", exc_info=True)
        print(f"\n❌ Error in pipeline diagnostics: {e}")
        return 2


if __name__ == "__main__":
    sys.exit(main())