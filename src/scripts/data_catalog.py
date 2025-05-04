#!/usr/bin/env python
"""
GitHub Archive Data Catalog Manager

Este módulo gestiona el registro de metadatos de los conjuntos de datos en un catálogo
de datos, facilitando la búsqueda, seguimiento y gobernanza de los datos.

Características:
- Registro de metadatos en catálogos locales o AWS Glue
- Seguimiento del linaje de datos
- Registro de esquemas y particiones
- Validación contra esquemas esperados
"""

import os
import sys
import json
import logging
import datetime
import hashlib
from pathlib import Path
from typing import Dict, Any, List, Optional, Union

import boto3
from botocore.exceptions import ClientError
import pandas as pd

# Configuración de logging
logger = logging.getLogger("data_catalog")

class DataCatalogManager:
    """
    Gestor de catálogo de datos que proporciona funcionalidades
    para registrar y gestionar metadatos de los conjuntos de datos.
    """
    
    def __init__(
        self,
        catalog_type: str = "local",
        catalog_config: Dict[str, Any] = None,
        metadata_dir: Path = Path("metadata")
    ):
        """
        Inicializa el gestor de catálogo de datos
        
        Args:
            catalog_type: Tipo de catálogo ('local', 'glue', 'datahub')
            catalog_config: Configuración específica del catálogo
            metadata_dir: Directorio para almacenar metadatos locales
        """
        self.catalog_type = catalog_type.lower()
        self.catalog_config = catalog_config or {}
        self.metadata_dir = metadata_dir
        
        # Crear directorio de metadatos si no existe
        if self.catalog_type == "local":
            self.metadata_dir.mkdir(parents=True, exist_ok=True)
        
        # Inicializar clientes específicos según el tipo de catálogo
        if self.catalog_type == "glue":
            self._init_glue_client()
    
    def _init_glue_client(self):
        """Inicializa el cliente de AWS Glue"""
        try:
            session_args = {}
            
            # Usar perfil si está especificado
            if self.catalog_config.get("profile"):
                session_args["profile_name"] = self.catalog_config["profile"]
            
            # Usar región si está especificada
            if self.catalog_config.get("region"):
                session_args["region_name"] = self.catalog_config["region"]
            
            session = boto3.Session(**session_args)
            self.glue_client = session.client('glue')
            
            logger.info("Cliente de AWS Glue inicializado exitosamente")
        except Exception as e:
            logger.error(f"Error al inicializar cliente de AWS Glue: {e}")
            raise
    
    def register_dataset(
        self,
        dataset_name: str,
        dataset_path: Union[str, Path],
        description: str = "",
        schema: Dict[str, Any] = None,
        partitions: List[Dict[str, Any]] = None,
        tags: Dict[str, str] = None,
        source_info: Dict[str, Any] = None,
        additional_metadata: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Registra un conjunto de datos en el catálogo
        
        Args:
            dataset_name: Nombre del conjunto de datos
            dataset_path: Ruta al conjunto de datos
            description: Descripción del conjunto de datos
            schema: Esquema del conjunto de datos
            partitions: Información de particiones
            tags: Etiquetas para el conjunto de datos
            source_info: Información sobre la fuente de los datos
            additional_metadata: Metadatos adicionales
            
        Returns:
            Metadatos del conjunto de datos registrado
        """
        # Convertir Path a string si es necesario
        if isinstance(dataset_path, Path):
            dataset_path = str(dataset_path)
        
        # Crear metadatos básicos
        current_time = datetime.datetime.now().isoformat()
        
        metadata = {
            "dataset_name": dataset_name,
            "dataset_path": dataset_path,
            "description": description,
            "schema": schema or {},
            "partitions": partitions or [],
            "tags": tags or {},
            "created_at": current_time,
            "updated_at": current_time,
            "source_info": source_info or {},
            "additional_metadata": additional_metadata or {}
        }
        
        # Registrar según el tipo de catálogo
        if self.catalog_type == "local":
            return self._register_local(dataset_name, metadata)
        elif self.catalog_type == "glue":
            return self._register_glue(dataset_name, metadata)
        else:
            logger.error(f"Tipo de catálogo no soportado: {self.catalog_type}")
            raise ValueError(f"Tipo de catálogo no soportado: {self.catalog_type}")
    
    def _register_local(self, dataset_name: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Registra metadatos en el catálogo local
        
        Args:
            dataset_name: Nombre del conjunto de datos
            metadata: Metadatos a registrar
            
        Returns:
            Metadatos registrados
        """
        # Generar nombre de archivo
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{dataset_name}_{timestamp}.json"
        metadata_path = self.metadata_dir / "datasets" / filename
        
        # Asegurar que el directorio existe
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Guardar metadatos
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"Metadatos registrados localmente en {metadata_path}")
        
        # Actualizar índice de conjuntos de datos
        self._update_dataset_index(dataset_name, metadata_path, metadata)
        
        return metadata
    
    def _update_dataset_index(
        self, 
        dataset_name: str, 
        metadata_path: Path, 
        metadata: Dict[str, Any]
    ):
        """
        Actualiza el índice de conjuntos de datos
        
        Args:
            dataset_name: Nombre del conjunto de datos
            metadata_path: Ruta al archivo de metadatos
            metadata: Metadatos del conjunto de datos
        """
        index_path = self.metadata_dir / "datasets" / "index.json"
        
        # Leer índice existente o crear uno nuevo
        if index_path.exists():
            with open(index_path, "r") as f:
                index = json.load(f)
        else:
            index = {"datasets": {}}
        
        # Actualizar índice
        index["datasets"][dataset_name] = {
            "latest_metadata_path": str(metadata_path),
            "updated_at": metadata["updated_at"],
            "tags": metadata.get("tags", {}),
            "description": metadata.get("description", "")
        }
        
        # Guardar índice actualizado
        with open(index_path, "w") as f:
            json.dump(index, f, indent=2)
        
        logger.info(f"Índice de conjuntos de datos actualizado")
    
    def _register_glue(self, dataset_name: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Registra metadatos en AWS Glue Data Catalog
        
        Args:
            dataset_name: Nombre del conjunto de datos
            metadata: Metadatos a registrar
            
        Returns:
            Metadatos registrados
        """
        try:
            # Verificar configuración necesaria
            database_name = self.catalog_config.get("database")
            if not database_name:
                raise ValueError("Se requiere un nombre de base de datos para registrar en Glue")
            
            # Verificar si la base de datos existe, crearla si no
            try:
                self.glue_client.get_database(Name=database_name)
                logger.info(f"Base de datos {database_name} encontrada en Glue")
            except ClientError as e:
                if e.response["Error"]["Code"] == "EntityNotFoundException":
                    logger.info(f"Base de datos {database_name} no encontrada, creándola")
                    self.glue_client.create_database(
                        DatabaseInput={
                            "Name": database_name,
                            "Description": f"Base de datos para {self.catalog_config.get('project_name', 'GitHub Archive')}"
                        }
                    )
                else:
                    raise
            
            # Construir tabla
            table_input = {
                "Name": dataset_name,
                "Description": metadata.get("description", ""),
                "StorageDescriptor": {
                    "Columns": self._convert_schema_to_glue_columns(metadata.get("schema", {})),
                    "Location": metadata["dataset_path"],
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "Compressed": True,
                    "SerdeInfo": {
                        "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe",
                        "Parameters": {
                            "paths": "*"
                        }
                    }
                },
                "PartitionKeys": self._convert_partitions_to_glue(metadata.get("partitions", [])),
                "TableType": "EXTERNAL_TABLE",
                "Parameters": {
                    "classification": "json",
                    "created_at": metadata["created_at"],
                    "updated_at": metadata["updated_at"]
                }
            }
            
            # Agregar etiquetas
            if metadata.get("tags"):
                table_input["Parameters"].update(metadata["tags"])
            
            # Crear o actualizar tabla
            try:
                self.glue_client.get_table(DatabaseName=database_name, Name=dataset_name)
                # Tabla ya existe, actualizarla
                logger.info(f"Tabla {dataset_name} encontrada en {database_name}, actualizándola")
                self.glue_client.update_table(
                    DatabaseName=database_name,
                    TableInput=table_input
                )
            except ClientError as e:
                if e.response["Error"]["Code"] == "EntityNotFoundException":
                    # Tabla no existe, crearla
                    logger.info(f"Tabla {dataset_name} no encontrada en {database_name}, creándola")
                    self.glue_client.create_table(
                        DatabaseName=database_name,
                        TableInput=table_input
                    )
                else:
                    raise
            
            # Actualizar particiones si está configurado
            if self.catalog_config.get("update_partitions", False) and metadata.get("partitions"):
                self._update_glue_partitions(database_name, dataset_name, metadata)
            
            logger.info(f"Metadatos registrados en AWS Glue: {database_name}.{dataset_name}")
            return metadata
            
        except Exception as e:
            logger.error(f"Error al registrar metadatos en AWS Glue: {e}")
            raise
    
    def _convert_schema_to_glue_columns(self, schema: Dict[str, Any]) -> List[Dict[str, str]]:
        """
        Convierte un esquema a formato de columnas de Glue
        
        Args:
            schema: Esquema del conjunto de datos
            
        Returns:
            Lista de columnas en formato Glue
        """
        columns = []
        
        for name, data_type in schema.items():
            # Mapear tipos de datos al formato de Glue
            glue_type = self._map_to_glue_type(data_type)
            
            columns.append({
                "Name": name,
                "Type": glue_type,
                "Comment": ""
            })
        
        return columns
    
    def _map_to_glue_type(self, data_type: str) -> str:
        """
        Mapea un tipo de datos a un tipo de Glue
        
        Args:
            data_type: Tipo de datos original
            
        Returns:
            Tipo de datos en formato Glue
        """
        # Mapa de tipos comunes
        type_map = {
            "string": "string",
            "str": "string",
            "text": "string",
            "int": "int",
            "integer": "int",
            "bigint": "bigint",
            "long": "bigint",
            "float": "float",
            "double": "double",
            "boolean": "boolean",
            "bool": "boolean",
            "date": "date",
            "timestamp": "timestamp",
            "datetime": "timestamp",
            "array": "array<string>",
            "map": "map<string,string>",
            "struct": "struct<>"
        }
        
        return type_map.get(data_type.lower(), "string")
    
    def _convert_partitions_to_glue(self, partitions: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        """
        Convierte definiciones de particiones al formato de Glue
        
        Args:
            partitions: Lista de definiciones de particiones
            
        Returns:
            Lista de columnas de partición en formato Glue
        """
        partition_keys = []
        
        for partition in partitions:
            partition_keys.append({
                "Name": partition["name"],
                "Type": self._map_to_glue_type(partition.get("type", "string")),
                "Comment": partition.get("description", "")
            })
        
        return partition_keys
    
    def _update_glue_partitions(
        self, 
        database_name: str, 
        table_name: str, 
        metadata: Dict[str, Any]
    ):
        """
        Actualiza las particiones de una tabla en Glue
        
        Args:
            database_name: Nombre de la base de datos
            table_name: Nombre de la tabla
            metadata: Metadatos del conjunto de datos
        """
        # Implementación específica para actualizar particiones
        # Depende de la estructura de particiones del proyecto
        pass
    
    def get_dataset_info(self, dataset_name: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene información sobre un conjunto de datos registrado
        
        Args:
            dataset_name: Nombre del conjunto de datos
            
        Returns:
            Metadatos del conjunto de datos o None si no se encuentra
        """
        if self.catalog_type == "local":
            return self._get_local_dataset_info(dataset_name)
        elif self.catalog_type == "glue":
            return self._get_glue_dataset_info(dataset_name)
        else:
            logger.error(f"Tipo de catálogo no soportado: {self.catalog_type}")
            return None
    
    def _get_local_dataset_info(self, dataset_name: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene información sobre un conjunto de datos del catálogo local
        
        Args:
            dataset_name: Nombre del conjunto de datos
            
        Returns:
            Metadatos del conjunto de datos o None si no se encuentra
        """
        index_path = self.metadata_dir / "datasets" / "index.json"
        
        if not index_path.exists():
            logger.warning(f"Índice de conjuntos de datos no encontrado: {index_path}")
            return None
        
        with open(index_path, "r") as f:
            index = json.load(f)
        
        if dataset_name not in index["datasets"]:
            logger.warning(f"Conjunto de datos no encontrado en el índice: {dataset_name}")
            return None
        
        dataset_entry = index["datasets"][dataset_name]
        metadata_path = dataset_entry["latest_metadata_path"]
        
        with open(metadata_path, "r") as f:
            metadata = json.load(f)
        
        return metadata
    
    def _get_glue_dataset_info(self, dataset_name: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene información sobre un conjunto de datos de AWS Glue
        
        Args:
            dataset_name: Nombre del conjunto de datos
            
        Returns:
            Metadatos del conjunto de datos o None si no se encuentra
        """
        try:
            database_name = self.catalog_config.get("database")
            if not database_name:
                logger.error("Se requiere un nombre de base de datos para consultar Glue")
                return None
            
            response = self.glue_client.get_table(
                DatabaseName=database_name,
                Name=dataset_name
            )
            
            table = response["Table"]
            
            # Convertir formato de Glue a formato interno
            metadata = {
                "dataset_name": table["Name"],
                "dataset_path": table["StorageDescriptor"]["Location"],
                "description": table.get("Description", ""),
                "schema": self._convert_glue_columns_to_schema(table["StorageDescriptor"]["Columns"]),
                "partitions": self._convert_glue_partitions(table.get("PartitionKeys", [])),
                "created_at": table["Parameters"].get("created_at", ""),
                "updated_at": table["Parameters"].get("updated_at", "")
            }
            
            return metadata
            
        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityNotFoundException":
                logger.warning(f"Tabla no encontrada en Glue: {dataset_name}")
                return None
            else:
                logger.error(f"Error al consultar Glue: {e}")
                raise
    
    def _convert_glue_columns_to_schema(self, columns: List[Dict[str, str]]) -> Dict[str, str]:
        """
        Convierte columnas de Glue a formato de esquema interno
        
        Args:
            columns: Lista de columnas de Glue
            
        Returns:
            Esquema en formato interno
        """
        schema = {}
        
        for column in columns:
            schema[column["Name"]] = column["Type"]
        
        return schema
    
    def _convert_glue_partitions(self, partitions: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """
        Convierte particiones de Glue a formato interno
        
        Args:
            partitions: Lista de particiones de Glue
            
        Returns:
            Lista de particiones en formato interno
        """
        result = []
        
        for partition in partitions:
            result.append({
                "name": partition["Name"],
                "type": partition["Type"],
                "description": partition.get("Comment", "")
            })
        
        return result
    
    def list_datasets(self) -> List[Dict[str, Any]]:
        """
        Lista todos los conjuntos de datos registrados
        
        Returns:
            Lista de información básica sobre los conjuntos de datos
        """
        if self.catalog_type == "local":
            return self._list_local_datasets()
        elif self.catalog_type == "glue":
            return self._list_glue_datasets()
        else:
            logger.error(f"Tipo de catálogo no soportado: {self.catalog_type}")
            return []
    
    def _list_local_datasets(self) -> List[Dict[str, Any]]:
        """
        Lista conjuntos de datos del catálogo local
        
        Returns:
            Lista de información básica sobre los conjuntos de datos
        """
        index_path = self.metadata_dir / "datasets" / "index.json"
        
        if not index_path.exists():
            logger.warning(f"Índice de conjuntos de datos no encontrado: {index_path}")
            return []
        
        with open(index_path, "r") as f:
            index = json.load(f)
        
        datasets = []
        for name, info in index.get("datasets", {}).items():
            datasets.append({
                "name": name,
                "updated_at": info.get("updated_at", ""),
                "description": info.get("description", ""),
                "tags": info.get("tags", {})
            })
        
        return datasets
    
    def _list_glue_datasets(self) -> List[Dict[str, Any]]:
        """
        Lista conjuntos de datos de AWS Glue
        
        Returns:
            Lista de información básica sobre los conjuntos de datos
        """
        try:
            database_name = self.catalog_config.get("database")
            if not database_name:
                logger.error("Se requiere un nombre de base de datos para consultar Glue")
                return []
            
            # Obtener tablas de la base de datos
            paginator = self.glue_client.get_paginator("get_tables")
            
            datasets = []
            for page in paginator.paginate(DatabaseName=database_name):
                for table in page["TableList"]:
                    datasets.append({
                        "name": table["Name"],
                        "updated_at": table["Parameters"].get("updated_at", ""),
                        "description": table.get("Description", ""),
                        "type": table.get("TableType", "")
                    })
            
            return datasets
            
        except ClientError as e:
            logger.error(f"Error al listar tablas de Glue: {e}")
            return []


# Función para crear una instancia del catálogo desde la configuración
def create_catalog_from_config(config: Dict[str, Any]) -> Optional[DataCatalogManager]:
    """
    Crea una instancia del catálogo de datos desde la configuración
    
    Args:
        config: Configuración del catálogo
        
    Returns:
        Instancia del catálogo o None si no está habilitado
    """
    if not config.get("enabled", False):
        logger.info("Catálogo de datos deshabilitado en la configuración")
        return None
    
    catalog_type = config.get("catalog_type", "local")
    
    try:
        return DataCatalogManager(
            catalog_type=catalog_type,
            catalog_config=config,
            metadata_dir=Path(config.get("metadata_dir", "metadata"))
        )
    except Exception as e:
        logger.error(f"Error al crear catálogo de datos: {e}")
        return None


# Ejemplo de uso
if __name__ == "__main__":
    # Configuración de prueba
    test_config = {
        "enabled": True,
        "catalog_type": "local",
        "metadata_dir": "metadata"
    }
    
    # Crear catálogo
    catalog = create_catalog_from_config(test_config)
    
    if catalog:
        # Registrar un conjunto de datos de prueba
        dataset_metadata = catalog.register_dataset(
            dataset_name="github_events_test",
            dataset_path="s3://test-bucket/github-archive/2023/05/01",
            description="Datos de eventos de GitHub para pruebas",
            schema={
                "id": "string",
                "type": "string",
                "actor": "struct",
                "repo": "struct",
                "created_at": "timestamp"
            },
            partitions=[
                {"name": "year", "type": "string"},
                {"name": "month", "type": "string"},
                {"name": "day", "type": "string"}
            ],
            tags={"source": "github_archive", "environment": "test"}
        )
        
        print(f"Conjunto de datos registrado: {dataset_metadata['dataset_name']}")
        
        # Listar conjuntos de datos
        datasets = catalog.list_datasets()
        print(f"Conjuntos de datos registrados: {len(datasets)}")
        for ds in datasets:
            print(f"  - {ds['name']}: {ds['description']}")
    else:
        print("No se pudo crear el catálogo de datos")