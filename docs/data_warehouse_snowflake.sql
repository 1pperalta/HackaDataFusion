-- 1. Crear y usar base de datos
CREATE OR REPLACE DATABASE GITHUB_DATA;
USE DATABASE GITHUB_DATA;

-- 2. Crear warehouse si no existe
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WITH WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE;

-- 3. Crear esquemas para cada capa
CREATE OR REPLACE SCHEMA BRONZE;
CREATE OR REPLACE SCHEMA SILVER;
CREATE OR REPLACE SCHEMA GOLD;

-- 4. Crear rol y asignar permisos (opcional)
CREATE ROLE IF NOT EXISTS GITHUB_ANALYTICS;
GRANT USAGE ON DATABASE GITHUB_DATA TO ROLE GITHUB_ANALYTICS;
GRANT USAGE ON SCHEMA GITHUB_DATA.BRONZE TO ROLE GITHUB_ANALYTICS;
GRANT USAGE ON SCHEMA GITHUB_DATA.SILVER TO ROLE GITHUB_ANALYTICS;
GRANT USAGE ON SCHEMA GITHUB_DATA.GOLD TO ROLE GITHUB_ANALYTICS;
GRANT CREATE TABLE ON SCHEMA GITHUB_DATA.BRONZE TO ROLE GITHUB_ANALYTICS;
GRANT CREATE TABLE ON SCHEMA GITHUB_DATA.SILVER TO ROLE GITHUB_ANALYTICS;
GRANT CREATE TABLE ON SCHEMA GITHUB_DATA.GOLD TO ROLE GITHUB_ANALYTICS;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE GITHUB_ANALYTICS;

-- 5. Usar esquema BRONZE
USE SCHEMA BRONZE;

-- 6. Crear tabla para eventos en la capa Bronze
CREATE OR REPLACE TABLE EVENTS (
    event_hash VARCHAR(32),
    file_name VARCHAR(255),
    file_date VARCHAR(255),
    processed_at VARCHAR(255),
    hour_bucket VARCHAR(255),
    raw_data VARIANT,
    event_id VARCHAR(255),
    event_type VARCHAR(255),
    created_at VARCHAR(255),
    actor_id NUMBER,
    actor_login VARCHAR(255),
    repo_id NUMBER,
    repo_name VARCHAR(255),
    payload VARIANT
);

-- 7. Crear estructura para capa Silver
USE SCHEMA SILVER;

-- Tabla para eventos normalizados
CREATE OR REPLACE TABLE SILVER_EVENTS (
    event_id VARCHAR(255),
    event_hash VARCHAR(32),
    event_type VARCHAR(255),
    actor_id NUMBER,
    actor_login VARCHAR(255),
    repo_id NUMBER,
    repo_name VARCHAR(255),
    created_at TIMESTAMP_NTZ,
    raw_data_json VARIANT,
    payload_json VARIANT,
    processed_at VARCHAR(255)
);

-- Tabla para repositorios 
CREATE OR REPLACE TABLE SILVER_REPOSITORIES (
    repo_id NUMBER,
    repo_name VARCHAR(255),
    owner VARCHAR(255),
    repo_short_name VARCHAR(255),
    first_seen_at TIMESTAMP_NTZ,
    last_seen_at TIMESTAMP_NTZ,
    event_count NUMBER
);

-- 8. Crear estructura para capa Gold
USE SCHEMA GOLD;

-- Tabla para actividad diaria
CREATE OR REPLACE TABLE DAILY_ACTIVITY (
    day DATE,
    event_type VARCHAR(255),
    event_count NUMBER,
    user_count NUMBER
);

-- Tabla para repositorios principales
CREATE OR REPLACE TABLE TOP_REPOSITORIES (
    repo_id NUMBER,
    repo_name VARCHAR(255),
    owner VARCHAR(255),
    repo_short_name VARCHAR(255),
    event_count NUMBER,
    unique_contributors NUMBER,
    last_activity TIMESTAMP_NTZ
);

-- 9. Conceder permisos adicionales si es necesario
GRANT SELECT ON ALL TABLES IN SCHEMA GITHUB_DATA.BRONZE TO ROLE GITHUB_ANALYTICS;
GRANT SELECT ON ALL TABLES IN SCHEMA GITHUB_DATA.SILVER TO ROLE GITHUB_ANALYTICS;
GRANT SELECT ON ALL TABLES IN SCHEMA GITHUB_DATA.GOLD TO ROLE GITHUB_ANALYTICS;

-- 10. Verificar la estructura creada
SHOW DATABASES LIKE 'GITHUB_DATA';
SHOW SCHEMAS IN DATABASE GITHUB_DATA;
SHOW TABLES IN SCHEMA GITHUB_DATA.BRONZE;
SHOW TABLES IN SCHEMA GITHUB_DATA.SILVER;
SHOW TABLES IN SCHEMA GITHUB_DATA.GOLD;

USE DATABASE GITHUB_DATA;
USE SCHEMA BRONZE;

-- Cambia al contexto adecuado
USE DATABASE GITHUB_DATA;
USE SCHEMA SILVER_SILVER;

-- Ver todas las tablas en el esquema
SHOW TABLES;

-- Examinar los datos de eventos
SELECT * FROM silver_events LIMIT 10;

-- Contar registros por tabla
SELECT COUNT(*) FROM silver_events;
SELECT COUNT(*) FROM silver_actors;
SELECT COUNT(*) FROM silver_repositories;
SELECT COUNT(*) FROM silver_organizations;
SELECT COUNT(*) FROM silver_payload_details;

-- Explorar la distribución de tipos de eventos
SELECT event_type, COUNT(*) AS event_count 
FROM silver_events 
GROUP BY event_type 
ORDER BY event_count DESC;





