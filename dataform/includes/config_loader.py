from google.cloud import storage
import json

def load_config():
    """Carga la configuración dinámica desde Cloud Storage"""
    client = storage.Client()
    bucket = client.bucket("ltm_migration")
    blob = bucket.blob("dataform_config/latest.json")
    
    try:
        content = blob.download_as_string()
        print("Contenido del JSON:", content)  # Debug
        config = json.loads(content)
        print("Compañías activas:", [c['id'] for c in config['active_companies']])  # Debug
        return config
    except Exception as e:
        raise ValueError(f"Error cargando configuración: {str(e)}")

# Variables globales
CONFIG = load_config()
ACTIVE_COMPANIES = CONFIG.get("active_companies", [])