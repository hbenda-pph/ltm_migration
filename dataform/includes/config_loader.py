from google.cloud import storage
import json

def load_config():
    """Carga la configuración dinámica desde Cloud Storage"""
    client = storage.Client()
    bucket = client.bucket("ltm_migration")
    blob = bucket.blob("dataform_config/latest.json")
    
    try:
        config = json.loads(blob.download_as_string())
        print(f"Configuración cargada: {len(config['active_companies'])} compañías")
        return config
    except Exception as e:
        raise ValueError(f"Error cargando configuración: {str(e)}")

# Variables globales
CONFIG = load_config()
ACTIVE_COMPANIES = CONFIG.get("active_companies", [])