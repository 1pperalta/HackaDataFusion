#!/usr/bin/env python
"""
GitHub Archive Data Validator

Este script realiza validaciones de calidad en los datos descargados de GitHub Archive,
verificando su integridad, completitud y conformidad con el esquema esperado.

Características:
- Validación estructural de archivos
- Verificación de completitud temporal
- Evaluación de esquema y tipos de datos
- Detección de valores atípicos y anomalías
- Generación de informes de calidad
"""

import os
import sys
import json
import gzip
import logging
import datetime
import argparse
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple, Set

import pandas as pd
from tqdm import tqdm

# Configuración de logging
logger = logging.getLogger("data_validator")

class GitHubArchiveValidator:
    """Validador de datos de GitHub Archive"""
    
    def __init__(
        self,
        data_dir: Path,
        output_dir: Optional[Path] = None,
        schema_file: Optional[Path] = None,
        min_file_size: int = 1000,  # 1KB
        sample_size: int = 100,  # Número de eventos a muestrear por archivo
        temporal_completeness: bool = True,
        max_errors_to_report: int = 20
    ):
        """
        Inicializa el validador de datos
        
        Args:
            data_dir: Directorio con datos a validar
            output_dir: Directorio para informes de validación
            schema_file: Archivo con definición de esquema esperado
            min_file_size: Tamaño mínimo esperado de archivos en bytes
            sample_size: Número de eventos a muestrear por archivo
            temporal_completeness: Si se debe verificar completitud temporal
            max_errors_to_report: Máximo número de errores a reportar
        """
        self.data_dir = data_dir
        self.output_dir = output_dir or Path("reports/validation")
        self.min_file_size = min_file_size
        self.sample_size = sample_size
        self.temporal_completeness = temporal_completeness
        self.max_errors_to_report = max_errors_to_report
        
        # Cargar esquema si se proporciona
        self.schema = None
        if schema_file:
            self._load_schema(schema_file)
        
        # Crear directorio de salida si no existe
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Preparar resultados de validación
        self.validation_results = {
            "timestamp": datetime.datetime.now().isoformat(),
            "data_directory": str(data_dir),
            "validation_summary": {
                "total_files": 0,
                "valid_files": 0,
                "invalid_files": 0,
                "warnings": 0,
                "errors": 0
            },
            "file_results": [],
            "temporal_coverage": {},
            "schema_validation": {},
            "data_quality_metrics": {}
        }
    
    def _load_schema(self, schema_file: Path):
        """
        Carga el esquema desde un archivo
        
        Args:
            schema_file: Ruta al archivo de esquema
        """
        try:
            with open(schema_file, "r") as f:
                self.schema = json.load(f)
            
            logger.info(f"Esquema cargado desde {schema_file}")
        except Exception as e:
            logger.error(f"Error al cargar el esquema: {e}")
            self.schema = None
    
    def find_files(self) -> List[Path]:
        """
        Encuentra archivos de GitHub Archive para validar
        
        Returns:
            Lista de rutas a los archivos
        """
        files = []
        
        # Buscar archivos .json.gz recursivamente
        for file_path in self.data_dir.glob("**/*.json.gz"):
            if file_path.is_file():
                files.append(file_path)
        
        return files
    
    def validate_file(self, file_path: Path) -> Dict[str, Any]:
        """
        Valida un archivo de GitHub Archive
        
        Args:
            file_path: Ruta al archivo
            
        Returns:
            Resultado de la validación
        """
        result = {
            "file_path": str(file_path),
            "relative_path": str(file_path.relative_to(self.data_dir)),
            "size_bytes": 0,
            "status": "pending",
            "errors": [],
            "warnings": [],
            "records_sampled": 0,
            "event_types": {},
            "actor_info": {},
            "repo_info": {}
        }
        
        try:
            # Verificar existencia
            if not file_path.exists():
                result["errors"].append("El archivo no existe")
                result["status"] = "error"
                return result
            
            # Verificar tamaño
            size = file_path.stat().st_size
            result["size_bytes"] = size
            
            if size < self.min_file_size:
                result["warnings"].append(f"Tamaño de archivo ({size} bytes) menor al mínimo esperado ({self.min_file_size} bytes)")
            
            # Validar formato gzip y JSON
            try:
                with gzip.open(file_path, "rt") as f:
                    # Muestrear eventos
                    events_sample = []
                    event_types = set()
                    actor_ids = set()
                    repo_ids = set()
                    
                    for i, line in enumerate(f):
                        if i >= self.sample_size:
                            break
                        
                        event = json.loads(line)
                        events_sample.append(event)
                        
                        # Recopilar estadísticas
                        event_type = event.get("type")
                        if event_type:
                            event_types.add(event_type)
                        
                        actor = event.get("actor", {})
                        if actor and isinstance(actor, dict):
                            actor_id = actor.get("id")
                            if actor_id:
                                actor_ids.add(actor_id)
                        
                        repo = event.get("repo", {})
                        if repo and isinstance(repo, dict):
                            repo_id = repo.get("id")
                            if repo_id:
                                repo_ids.add(repo_id)
                    
                    # Actualizar resultado
                    result["records_sampled"] = len(events_sample)
                    result["event_types"] = {t: events_sample.count(lambda e: e.get("type") == t) for t in event_types}
                    result["actor_info"] = {"unique_actors": len(actor_ids)}
                    result["repo_info"] = {"unique_repos": len(repo_ids)}
                    
                    # Validar contra esquema si está disponible
                    if self.schema:
                        schema_errors = self._validate_against_schema(events_sample)
                        if schema_errors:
                            result["errors"].extend(schema_errors[:self.max_errors_to_report])
                            if len(schema_errors) > self.max_errors_to_report:
                                result["warnings"].append(f"Se omitieron {len(schema_errors) - self.max_errors_to_report} errores adicionales de esquema")
            
            except gzip.BadGzipFile:
                result["errors"].append("Archivo gzip inválido")
                result["status"] = "error"
                return result
            
            except json.JSONDecodeError as e:
                result["errors"].append(f"JSON inválido: {e}")
                result["status"] = "error"
                return result
            
            # Determinar estado final
            if result["errors"]:
                result["status"] = "error"
            elif result["warnings"]:
                result["status"] = "warning"
            else:
                result["status"] = "valid"
            
            return result
            
        except Exception as e:
            result["errors"].append(f"Error durante la validación: {str(e)}")
            result["status"] = "error"
            return result
    
    def _validate_against_schema(self, events: List[Dict[str, Any]]) -> List[str]:
        """
        Valida eventos contra el esquema definido
        
        Args:
            events: Lista de eventos a validar
            
        Returns:
            Lista de errores encontrados
        """
        errors = []
        
        # Implementación simplificada - en un caso real sería más sofisticada
        required_fields = self.schema.get("required_fields", [])
        field_types = self.schema.get("field_types", {})
        
        for i, event in enumerate(events):
            # Verificar campos requeridos
            for field in required_fields:
                if field not in event:
                    errors.append(f"Evento {i}: Campo requerido '{field}' no encontrado")
            
            # Verificar tipos de datos
            for field, expected_type in field_types.items():
                if field in event:
                    value = event[field]
                    
                    # Validar tipo
                    if expected_type == "string" and not isinstance(value, str):
                        errors.append(f"Evento {i}: Campo '{field}' debe ser string, es {type(value).__name__}")
                    
                    elif expected_type == "number" and not isinstance(value, (int, float)):
                        errors.append(f"Evento {i}: Campo '{field}' debe ser número, es {type(value).__name__}")
                    
                    elif expected_type == "boolean" and not isinstance(value, bool):
                        errors.append(f"Evento {i}: Campo '{field}' debe ser boolean, es {type(value).__name__}")
                    
                    elif expected_type == "object" and not isinstance(value, dict):
                        errors.append(f"Evento {i}: Campo '{field}' debe ser objeto, es {type(value).__name__}")
                    
                    elif expected_type == "array" and not isinstance(value, list):
                        errors.append(f"Evento {i}: Campo '{field}' debe ser array, es {type(value).__name__}")
        
        return errors
    
    def validate_temporal_coverage(self, files: List[Path]) -> Dict[str, Any]:
        """
        Valida la completitud temporal de los archivos
        
        Args:
            files: Lista de archivos encontrados
            
        Returns:
            Resultado de la validación temporal
        """
        if not self.temporal_completeness:
            return {"enabled": False}
        
        result = {
            "enabled": True,
            "start_date": None,
            "end_date": None,
            "expected_hours": 0,
            "found_hours": 0,
            "missing_hours": [],
            "coverage_percentage": 0.0
        }
        
        try:
            # Extraer fechas de nombres de archivos
            # Ejemplo: 2023/05/01/2023-05-01-15.json.gz
            
            all_hours = set()
            for file_path in files:
                try:
                    # Extraer componentes de fecha del nombre
                    filename = file_path.name
                    if not filename.endswith(".json.gz"):
                        continue
                    
                    # Formato esperado: YYYY-MM-DD-H.json.gz
                    date_part = filename.split(".")[0]  # 2023-05-01-15
                    
                    # Extraer componentes
                    year, month, day, hour = date_part.split("-")
                    
                    # Crear clave para esta hora
                    hour_key = f"{year}-{month}-{day}-{hour}"
                    all_hours.add(hour_key)
                    
                    # Actualizar fechas límite
                    file_date = datetime.datetime(int(year), int(month), int(day), int(hour))
                    
                    if result["start_date"] is None or file_date < result["start_date"]:
                        result["start_date"] = file_date
                    
                    if result["end_date"] is None or file_date > result["end_date"]:
                        result["end_date"] = file_date
                    
                except (ValueError, IndexError):
                    continue
            
            # Si no hay fechas, salir
            if not result["start_date"] or not result["end_date"]:
                return result
            
            # Calcular horas esperadas
            start_date = result["start_date"]
            end_date = result["end_date"]
            
            # Convertir a formato string para el resultado
            result["start_date"] = start_date.isoformat()
            result["end_date"] = end_date.isoformat()
            
            # Generar todas las horas esperadas
            expected_hours = set()
            current_hour = start_date
            
            while current_hour <= end_date:
                hour_key = current_hour.strftime("%Y-%m-%d-%H")
                expected_hours.add(hour_key)
                current_hour += datetime.timedelta(hours=1)
            
            # Calcular horas faltantes
            missing_hours = expected_hours - all_hours
            
            # Actualizar resultado
            result["expected_hours"] = len(expected_hours)
            result["found_hours"] = len(all_hours)
            result["missing_hours"] = sorted(list(missing_hours))
            
            if len(expected_hours) > 0:
                result["coverage_percentage"] = (len(all_hours) / len(expected_hours)) * 100
            
            return result
            
        except Exception as e:
            logger.error(f"Error al validar cobertura temporal: {e}")
            result["error"] = str(e)
            return result
    
    def run(self) -> Dict[str, Any]:
        """
        Ejecuta todas las validaciones
        
        Returns:
            Resultados completos de la validación
        """
        start_time = datetime.datetime.now()
        logger.info(f"Iniciando validación de datos en {self.data_dir}")
        
        # Encontrar archivos
        files = self.find_files()
        file_count = len(files)
        
        if file_count == 0:
            logger.warning(f"No se encontraron archivos para validar en {self.data_dir}")
            self.validation_results["validation_summary"]["status"] = "error"
            self.validation_results["validation_summary"]["message"] = "No se encontraron archivos para validar"
            return self.validation_results
        
        logger.info(f"Encontrados {file_count} archivos para validar")
        
        # Validar cada archivo
        with tqdm(total=file_count, desc="Validando archivos") as pbar:
            for file_path in files:
                result = self.validate_file(file_path)
                self.validation_results["file_results"].append(result)
                
                # Actualizar contadores
                self.validation_results["validation_summary"]["total_files"] += 1
                
                if result["status"] == "valid":
                    self.validation_results["validation_summary"]["valid_files"] += 1
                else:
                    self.validation_results["validation_summary"]["invalid_files"] += 1
                
                self.validation_results["validation_summary"]["warnings"] += len(result["warnings"])
                self.validation_results["validation_summary"]["errors"] += len(result["errors"])
                
                pbar.update(1)
        
        # Validar cobertura temporal
        if self.temporal_completeness:
            logger.info("Validando cobertura temporal")
            self.validation_results["temporal_coverage"] = self.validate_temporal_coverage(files)
        
        # Calcular métricas generales
        self._calculate_overall_metrics()
        
        # Finalizar validación
        end_time = datetime.datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        self.validation_results["duration_seconds"] = duration
        self.validation_results["end_timestamp"] = end_time.isoformat()
        
        # Determinar estado general
        if self.validation_results["validation_summary"]["errors"] > 0:
            status = "error"
        elif self.validation_results["validation_summary"]["warnings"] > 0:
            status = "warning"
        else:
            status = "success"
        
        self.validation_results["validation_summary"]["status"] = status
        
        # Guardar resultados
        self._save_results()
        
        logger.info(f"Validación completada en {duration:.2f} segundos. Estado: {status}")
        return self.validation_results
    
    def _calculate_overall_metrics(self):
        """Calcula métricas generales de calidad de datos"""
        
        try:
            file_results = self.validation_results["file_results"]
            
            if not file_results:
                return
            
            # Recopilar tipos de eventos
            all_event_types = {}
            for result in file_results:
                for event_type, count in result.get("event_types", {}).items():
                    all_event_types[event_type] = all_event_types.get(event_type, 0) + count
            
            # Calcular distribución de tamaños
            file_sizes = [result["size_bytes"] for result in file_results]
            
            # Métricas básicas
            self.validation_results["data_quality_metrics"] = {
                "event_types": all_event_types,
                "file_sizes": {
                    "min": min(file_sizes) if file_sizes else 0,
                    "max": max(file_sizes) if file_sizes else 0,
                    "avg": sum(file_sizes) / len(file_sizes) if file_sizes else 0
                },
                "total_errors": self.validation_results["validation_summary"]["errors"],
                "total_warnings": self.validation_results["validation_summary"]["warnings"],
                "validation_rate": (self.validation_results["validation_summary"]["valid_files"] / 
                                   self.validation_results["validation_summary"]["total_files"]) * 100
                                   if self.validation_results["validation_summary"]["total_files"] > 0 else 0
            }
        
        except Exception as e:
            logger.error(f"Error al calcular métricas: {e}")
            self.validation_results["data_quality_metrics"] = {
                "error": str(e)
            }
    
    def _save_results(self):
        """Guarda los resultados de la validación en un archivo"""
        
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        result_file = self.output_dir / f"validation_results_{timestamp}.json"
        
        with open(result_file, "w") as f:
            json.dump(self.validation_results, f, indent=2)
        
        logger.info(f"Resultados guardados en {result_file}")


def parse_arguments():
    """
    Parsea los argumentos de línea de comandos
    
    Returns:
        Argumentos parseados
    """
    parser = argparse.ArgumentParser(description="Validador de datos de GitHub Archive")
    
    parser.add_argument(
        "--data-dir",
        type=str,
        default="data/raw",
        help="Directorio con datos a validar (default: data/raw)"
    )
    
    parser.add_argument(
        "--output-dir",
        type=str,
        default="reports/validation",
        help="Directorio para informes de validación (default: reports/validation)"
    )
    
    parser.add_argument(
        "--schema-file",
        type=str,
        help="Archivo con definición de esquema esperado (opcional)"
    )
    
    parser.add_argument(
        "--min-file-size",
        type=int,
        default=1000,
        help="Tamaño mínimo esperado de archivos en bytes (default: 1000)"
    )
    
    parser.add_argument(
        "--sample-size",
        type=int,
        default=100,
        help="Número de eventos a muestrear por archivo (default: 100)"
    )
    
    parser.add_argument(
        "--no-temporal-check",
        action="store_true",
        help="Desactivar verificación de completitud temporal"
    )
    
    return parser.parse_args()


def main():
    """Función principal"""
    try:
        # Parsear argumentos
        args = parse_arguments()
        
        # Configurar validator
        validator = GitHubArchiveValidator(
            data_dir=Path(args.data_dir),
            output_dir=Path(args.output_dir),
            schema_file=Path(args.schema_file) if args.schema_file else None,
            min_file_size=args.min_file_size,
            sample_size=args.sample_size,
            temporal_completeness=not args.no_temporal_check
        )
        
        # Ejecutar validación
        results = validator.run()
        
        # Mostrar resumen
        status = results["validation_summary"]["status"]
        valid_files = results["validation_summary"]["valid_files"]
        total_files = results["validation_summary"]["total_files"]
        errors = results["validation_summary"]["errors"]
        warnings = results["validation_summary"]["warnings"]
        
        if status == "success":
            print(f"\n✅ Validación exitosa: {valid_files}/{total_files} archivos válidos")
        elif status == "warning":
            print(f"\n⚠️ Validación con advertencias: {valid_files}/{total_files} archivos válidos, {warnings} advertencias")
        else:
            print(f"\n❌ Validación con errores: {valid_files}/{total_files} archivos válidos, {errors} errores")
        
        if "temporal_coverage" in results and results["temporal_coverage"].get("enabled", False):
            coverage = results["temporal_coverage"].get("coverage_percentage", 0)
            missing = len(results["temporal_coverage"].get("missing_hours", []))
            
            print(f"📊 Cobertura temporal: {coverage:.1f}% ({missing} horas faltantes)")
        
        print(f"📝 Informe guardado en {args.output_dir}")
        
        # Salir con código según resultado
        return 0 if status != "error" else 1
    
    except KeyboardInterrupt:
        print("\n⚠️ Validación interrumpida por el usuario")
        return 130
    
    except Exception as e:
        print(f"\n❌ Error durante la validación: {e}")
        return 2


if __name__ == "__main__":
    sys.exit(main())