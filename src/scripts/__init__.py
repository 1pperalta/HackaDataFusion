"""
Módulos de scripts auxiliares para el pipeline de datos.

Este paquete contiene utilidades y scripts para diversas tareas auxiliares
en el pipeline de ingestión de datos.
"""

from .data_catalog import DataCatalogManager, create_catalog_from_config

__all__ = ['DataCatalogManager', 'create_catalog_from_config']