# test_snowflake.py
import snowflake.connector

# Conectar a Snowflake con valores directos
try:
    print("Intentando conectar a Snowflake...")
    conn = snowflake.connector.connect(
        user='AARIAS04',
        password='MinisterArias04',  # Reemplaza con tu contraseña real
        account='idlkunk-ax51793',
        warehouse='COMPUTE_WH',
        database='GITHUB_DATA',
        schema='BRONZE',
        role='ACCOUNTADMIN'
    )
    
    cursor = conn.cursor()
    cursor.execute('SELECT current_version()')
    version = cursor.fetchone()[0]
    print(f"¡Conexión exitosa! Versión de Snowflake: {version}")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"Error conectando a Snowflake: {e}")