import os
import gzip
import json
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
from tqdm import tqdm
import hashlib
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('bronze_processor')

class BronzeProcessor:
    """
    Clase para procesar archivos JSON de GitHub Events, cargarlos a Snowflake
    y guardarlos en formato Parquet en la capa Bronze
    """
    
    def __init__(
        self, 
        raw_data_path: str = 'data/raw',
        bronze_data_path: str = 'data/processed/bronze',
        processed_files_path: str = 'data/processed_files.txt',
        load_to_snowflake: bool = True,
        save_to_parquet: bool = True,
        snowflake_config: Optional[Dict[str, str]] = None
    ):
        """
        Inicializa el procesador de la capa Bronze
        
        Args:
            raw_data_path: Ruta a los archivos JSON de eventos de GitHub
            bronze_data_path: Ruta donde guardar los archivos Parquet procesados
            processed_files_path: Ruta al archivo que guarda los archivos ya procesados
            load_to_snowflake: Si se deben cargar los datos a Snowflake
            save_to_parquet: Si se deben guardar los datos en formato Parquet
            snowflake_config: Configuración de conexión a Snowflake
        """
        self.raw_data_path = Path(raw_data_path)
        self.bronze_data_path = Path(bronze_data_path)
        self.processed_files_path = Path(processed_files_path)
        self.load_to_snowflake = load_to_snowflake
        self.save_to_parquet = save_to_parquet
        
        # Asegurar que existan los directorios necesarios
        self.bronze_data_path.mkdir(parents=True, exist_ok=True)
        self.processed_files_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Configuración para Snowflake
        self.snowflake_config = {
            'account': 'idlkunk-ax51793',  # Reemplazar con tu cuenta real
            'user': 'AARIAS04',            # Reemplazar con tu usuario real
            'password': 'MinisterArias04',              # Reemplazar con tu contraseña real
            'warehouse': 'COMPUTE_WH',
            'database': 'GITHUB_DATA',
            'schema': 'BRONZE',
            'role': 'ACCOUNTADMIN'
        }
        
        # Actualizar con la configuración proporcionada
        if snowflake_config:
            self.snowflake_config.update(snowflake_config)
        
        # Cargar los archivos ya procesados
        self.processed_files = self._load_processed_files()
        
    def _load_processed_files(self) -> List[str]:
        """
        Carga la lista de archivos ya procesados
        
        Returns:
            Lista de archivos ya procesados
        """
        if not self.processed_files_path.exists():
            return []
        
        with open(self.processed_files_path, 'r') as f:
            return [line.strip() for line in f.readlines()]
    
    def _save_processed_files(self) -> None:
        """
        Guarda la lista de archivos procesados al disco
        """
        with open(self.processed_files_path, 'w') as f:
            for file_path in self.processed_files:
                f.write(f"{file_path}\n")
    
    def _mark_file_as_processed(self, file_path: str) -> None:
        """
        Marca un archivo como procesado
        
        Args:
            file_path: Ruta del archivo procesado
        """
        self.processed_files.append(file_path)
        self._save_processed_files()
    
    def _extract_date_from_filename(self, filename: str) -> Optional[str]:
        """
        Extrae la fecha de un nombre de archivo
        
        Args:
            filename: Nombre del archivo
            
        Returns:
            Fecha extraída del nombre de archivo o None si no se puede extraer
        """
        # Formato esperado: yyyy-mm-dd-HH.json.gz
        try:
            parts = Path(filename).stem.split('.')
            date_parts = parts[0].split('-')
            if len(date_parts) >= 3:
                return f"{date_parts[0]}-{date_parts[1]}-{date_parts[2]}"
            return None
        except Exception as e:
            logger.error(f"Error extrayendo fecha de {filename}: {e}")
            return None
    
    def _extract_hour_from_filename(self, filename: str) -> Optional[str]:
        """
        Extrae la hora de un nombre de archivo
        
        Args:
            filename: Nombre del archivo
            
        Returns:
            Hora extraída del nombre de archivo o None si no se puede extraer
        """
        # Formato esperado: yyyy-mm-dd-HH.json.gz
        try:
            parts = Path(filename).stem.split('.')
            date_parts = parts[0].split('-')
            if len(date_parts) >= 4:
                return date_parts[3]
            return None
        except Exception as e:
            logger.error(f"Error extrayendo hora de {filename}: {e}")
            return None
    
    def _extract_date_components(self, file_path: str) -> Dict[str, str]:
        """
        Extrae componentes de fecha de una ruta de archivo
        
        Args:
            file_path: Ruta del archivo
            
        Returns:
            Diccionario con componentes de fecha (year, month, day, hour)
        """
        date_str = self._extract_date_from_filename(file_path)
        hour_str = self._extract_hour_from_filename(file_path)
        
        date_components = {'year': None, 'month': None, 'day': None, 'hour': None}
        
        # Extraer componentes de la fecha
        if date_str:
            try:
                date_parts = date_str.split('-')
                if len(date_parts) >= 3:
                    date_components['year'] = date_parts[0]
                    date_components['month'] = date_parts[1].zfill(2)
                    date_components['day'] = date_parts[2].zfill(2)
            except Exception as e:
                logger.warning(f"Error al analizar la fecha {date_str}: {e}")
        
        # Extraer hora
        if hour_str:
            date_components['hour'] = hour_str.zfill(2)
        
        # Extraer de la ruta si no se pudo extraer del nombre de archivo
        if not all([date_components['year'], date_components['month'], date_components['day']]):
            # Intentar extraer de la estructura de directorios (año/mes/día)
            path_parts = Path(file_path).parts
            
            for part in path_parts:
                # Buscar año (4 dígitos entre 2000 y 2030)
                if part.isdigit() and len(part) == 4 and 2000 <= int(part) <= 2030:
                    date_components['year'] = part
                # Buscar mes (1-2 dígitos entre 1 y 12)
                elif part.isdigit() and len(part) <= 2 and 1 <= int(part) <= 12:
                    date_components['month'] = part.zfill(2)
                # Buscar día (1-2 dígitos entre 1 y 31)
                elif part.isdigit() and len(part) <= 2 and 1 <= int(part) <= 31:
                    date_components['day'] = part.zfill(2)
        
        # Valores predeterminados para componentes faltantes
        if not date_components['year'] or not date_components['month'] or not date_components['day']:
            today = datetime.now()
            if not date_components['year']:
                date_components['year'] = str(today.year)
            if not date_components['month']:
                date_components['month'] = str(today.month).zfill(2)
            if not date_components['day']:
                date_components['day'] = str(today.day).zfill(2)
        
        if not date_components['hour']:
            date_components['hour'] = '00'
        
        return date_components
    
    def get_files_to_process(self) -> List[str]:
        """
        Obtiene la lista de archivos a procesar
        
        Returns:
            Lista de rutas de archivos a procesar
        """
        all_files = []
        # Buscar archivos .json.gz recursivamente
        for file_path in self.raw_data_path.glob('**/*.json.gz'):
            all_files.append(str(file_path))
        
        # Filtrar archivos ya procesados
        files_to_process = [f for f in all_files if f not in self.processed_files]
        return files_to_process
    
    def process_event(self, event: Dict[str, Any], file_path: str) -> Dict[str, Any]:
        """
        Procesa un evento de GitHub para la capa Bronze
        
        Args:
            event: Evento de GitHub en formato JSON
            file_path: Ruta del archivo de origen
            
        Returns:
            Evento procesado
        """
        # Extraer componentes de fecha
        date_str = self._extract_date_from_filename(file_path)
        hour_str = self._extract_hour_from_filename(file_path)
        
        # Extraer datos relevantes del evento
        processed_event = {
            'file_name': str(file_path),
            'file_date': date_str,
            'hour_bucket': hour_str,
            'processed_at': datetime.now().isoformat(),
            'raw_data': json.dumps(event),  # Guardar el evento completo como JSON
            'event_id': event.get('id'),
            'event_type': event.get('type'),
            'created_at': event.get('created_at'),
            'actor_id': event.get('actor', {}).get('id'),
            'actor_login': event.get('actor', {}).get('login'),
            'repo_id': event.get('repo', {}).get('id'),
            'repo_name': event.get('repo', {}).get('name'),
            'payload': json.dumps(event.get('payload', {}))
        }
        
        # Crear un hash único para el evento
        event_str = json.dumps(event, sort_keys=True)
        event_hash = hashlib.md5(event_str.encode()).hexdigest()
        processed_event['event_hash'] = event_hash
        
        return processed_event
    
    def load_to_snowflake_table(self, df: pd.DataFrame) -> bool:
        """
        Carga un DataFrame a una tabla de Snowflake
        
        Args:
            df: DataFrame a cargar
            
        Returns:
            True si la carga fue exitosa
        """
        if not self.load_to_snowflake:
            return False
        
        logger.info(f"Cargando {len(df)} filas a Snowflake...")
        
        try:
            # Conectar a Snowflake
            conn = snowflake.connector.connect(
                user=self.snowflake_config['user'],
                password=self.snowflake_config['password'],
                account=self.snowflake_config['account'],
                warehouse=self.snowflake_config['warehouse'],
                database=self.snowflake_config['database'],
                schema=self.snowflake_config['schema'],
                role=self.snowflake_config['role']
            )
            
            # Preparar datos para Snowflake
            df_copy = df.copy()
            
            # Normalizar nombres de columnas (todos a mayúsculas, sin comillas)
            df_copy.columns = [col.upper() for col in df_copy.columns]
            
            # Asegurarse que las columnas JSON estén en formato string
            if 'RAW_DATA' in df_copy.columns and df_copy['RAW_DATA'].dtype == 'object':
                df_copy['RAW_DATA'] = df_copy['RAW_DATA'].astype(str)
            
            if 'PAYLOAD' in df_copy.columns and df_copy['PAYLOAD'].dtype == 'object':
                df_copy['PAYLOAD'] = df_copy['PAYLOAD'].astype(str)
            
            # Crear la tabla si no existe
            cursor = conn.cursor()
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS EVENTS (
                EVENT_HASH VARCHAR(32),
                FILE_NAME VARCHAR(255),
                FILE_DATE VARCHAR(255),
                PROCESSED_AT VARCHAR(255),
                HOUR_BUCKET VARCHAR(255),
                RAW_DATA VARIANT,
                EVENT_ID VARCHAR(255),
                EVENT_TYPE VARCHAR(255),
                CREATED_AT VARCHAR(255),
                ACTOR_ID NUMBER,
                ACTOR_LOGIN VARCHAR(255),
                REPO_ID NUMBER,
                REPO_NAME VARCHAR(255),
                PAYLOAD VARIANT
            )
            """)
            
            # Cargar datos a Snowflake
            success, num_chunks, num_rows, _ = write_pandas(
                conn, 
                df_copy, 
                table_name="EVENTS",
                auto_create_table=False,
                quote_identifiers=False
            )
            
            logger.info(f"Datos cargados a Snowflake: {num_rows} filas en {num_chunks} chunks")
            
            # Cerrar conexión
            cursor.close()
            conn.close()
            
            return True
            
        except Exception as e:
            logger.error(f"Error cargando datos a Snowflake: {e}", exc_info=True)
            return False
    
    def save_to_parquet_file(self, df: pd.DataFrame, file_path: str) -> bool:
        """
        Guarda un DataFrame en formato Parquet
        
        Args:
            df: DataFrame a guardar
            file_path: Ruta del archivo de origen
            
        Returns:
            True si se guardó correctamente
        """
        if not self.save_to_parquet:
            return False
        
        # Extraer componentes de fecha para la estructura de directorios
        date_components = self._extract_date_components(file_path)
        year = date_components['year']
        month = date_components['month']
        day = date_components['day']
        hour = date_components['hour']
        
        # Crear directorios si no existen
        output_dir = self.bronze_data_path / year / month / day
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Nombre del archivo de salida
        file_name = f"github_events_{year}-{month}-{day}-{hour}.parquet"
        output_path = output_dir / file_name
        
        try:
            # Guardar como Parquet
            df.to_parquet(
                output_path,
                compression='snappy',
                index=False
            )
            logger.info(f"Datos guardados en formato Parquet: {output_path}")
            return True
        except Exception as e:
            logger.error(f"Error guardando datos en Parquet: {e}", exc_info=True)
            return False
    
    def process_file(self, file_path: str) -> Dict[str, Any]:
        """
        Procesa un archivo de eventos de GitHub
        
        Args:
            file_path: Ruta al archivo a procesar
            
        Returns:
            Diccionario con resultados del procesamiento
        """
        logger.info(f"Procesando archivo: {file_path}")
        
        try:
            events = []
            batch_size = 10000
            batch_count = 0
            total_events = 0
            
            # Abrir y leer el archivo comprimido
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                for line in f:
                    try:
                        event = json.loads(line)
                        processed_event = self.process_event(event, file_path)
                        events.append(processed_event)
                        
                        # Procesar en lotes para evitar problemas de memoria
                        if len(events) >= batch_size:
                            batch_count += 1
                            total_events += len(events)
                            batch_df = pd.DataFrame(events)
                            
                            # Guardar en Parquet
                            if self.save_to_parquet:
                                self.save_to_parquet_file(batch_df, file_path)
                            
                            # Cargar a Snowflake
                            if self.load_to_snowflake:
                                self.load_to_snowflake_table(batch_df)
                                
                            events = []
                    except json.JSONDecodeError:
                        logger.warning(f"Línea inválida en {file_path}, saltando...")
                        continue
                
                # Procesar el último lote
                if events:
                    batch_count += 1
                    total_events += len(events)
                    batch_df = pd.DataFrame(events)
                    
                    # Guardar en Parquet
                    if self.save_to_parquet:
                        self.save_to_parquet_file(batch_df, file_path)
                    
                    # Cargar a Snowflake
                    if self.load_to_snowflake:
                        self.load_to_snowflake_table(batch_df)
            
            # Marcar archivo como procesado
            self._mark_file_as_processed(file_path)
            
            logger.info(f"Archivo procesado correctamente: {file_path}, {batch_count} lotes, {total_events} eventos")
            return {
                'success': True,
                'file_path': file_path,
                'batch_count': batch_count,
                'total_events': total_events
            }
            
        except Exception as e:
            logger.error(f"Error procesando archivo {file_path}: {e}", exc_info=True)
            return {
                'success': False,
                'file_path': file_path,
                'error': str(e)
            }
    
    def run(self) -> Dict[str, int]:
        """
        Ejecuta el procesamiento de todos los archivos pendientes
        
        Returns:
            Diccionario con estadísticas del procesamiento
        """
        files = self.get_files_to_process()
        successful_files = 0
        failed_files = 0
        total_events = 0
        
        if not files:
            logger.info("No hay archivos nuevos para procesar")
            return {
                'successful_files': 0,
                'failed_files': 0,
                'skipped_files': 0,
                'total_events': 0
            }
        
        logger.info(f"Procesando {len(files)} archivos...")
        
        # Procesar archivos con barra de progreso
        for file_path in tqdm(files, desc="Procesando archivos"):
            if file_path in self.processed_files:
                continue
                
            result = self.process_file(file_path)
            if result['success']:
                successful_files += 1
                total_events += result.get('total_events', 0)
            else:
                failed_files += 1
        
        logger.info(f"Procesamiento completado: {successful_files}/{len(files)} archivos exitosos, {total_events} eventos")
        
        return {
            'successful_files': successful_files,
            'failed_files': failed_files,
            'skipped_files': 0,
            'total_events': total_events
        }

# Ejecutar el procesamiento si se ejecuta como script principal
if __name__ == "__main__":
    processor = BronzeProcessor(
        raw_data_path='data/raw',
        bronze_data_path='data/processed/bronze',
        processed_files_path='data/processed_files.txt',
        load_to_snowflake=True,  # Cargar a Snowflake
        save_to_parquet=True     # También guardar en formato Parquet localmente
    )
    results = processor.run()
    
    print("\n✅ Procesamiento completado:")
    print(f"📊 {results['successful_files']}/{results['successful_files'] + results['failed_files']} archivos procesados correctamente")
    print(f"⚠️ {results['failed_files']} archivos fallidos")
    print(f"📝 {results['skipped_files']} archivos omitidos (ya procesados)")
    print(f"🔢 Total de eventos procesados: {results['total_events']}")