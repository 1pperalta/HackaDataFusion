import os
import logging
import subprocess
from datetime import datetime

# Configurar el logger del pipeline principal
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
log_dir = os.path.join(project_root, 'logs')
os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(log_dir, 'pipeline.log')

logging.basicConfig(
    filename=log_file,
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s - pipeline - %(levelname)s - %(message)s'
)

def log_info(msg):
    print(msg)
    logging.info(msg)

def log_error(msg):
    print(msg)
    logging.error(msg)

def ejecutar_comando(comando):
    try:
        result = subprocess.run(
            comando,
            shell=True,
            capture_output=True,
            text=True
        )
        if result.stdout:
            for line in result.stdout.strip().split("\n"):
                log_info(f"[OUT] {line}")
        if result.stderr:
            for line in result.stderr.strip().split("\n"):
                log_error(f"[ERR] {line}")
        return result.returncode == 0
    except Exception as e:
        log_error(f"Excepción al ejecutar comando: {e}")
        return False

def ejecutar_pipeline(pipeline_name):
    """
    Ejecuta un pipeline dado y maneja errores.
    """
    log_info(f"🟡 Ejecutando pipeline: {pipeline_name}...")
    pipeline_path = os.path.join(project_root, f"src/pipeline/{pipeline_name}")  # Asegúrate de que la ruta sea correcta
    comando = f"python {pipeline_path}"
    if not ejecutar_comando(comando):
        log_error(f"❌ Error al ejecutar {pipeline_name}. Abortando.")
        return False
    log_info(f"✅ {pipeline_name} ejecutado exitosamente.")
    return True

def main():
    log_info(f"Raíz del proyecto: {project_root}")
    log_info("🟡 Iniciando ejecución del pipeline maestro")

    # Ejecutar ingest_pipeline.py
    if not ejecutar_pipeline("ingest_pipeline.py"):
        log_error("Error en el pipeline de ingest. Abortando.")
        return

    # Ejecutar medalion_pipeline.py
    if not ejecutar_pipeline("medalion_pipeline.py"):
        log_error("Error en el pipeline de medalion. Abortando.")
        return

    # Ejecutar model_pipeline.py
    if not ejecutar_pipeline("model_pipeline.py"):
        log_error("Error en el pipeline de model. Abortando.")
        return

    log_info("🟢 Pipeline maestro finalizado correctamente.")

if __name__ == "__main__":
    main()
