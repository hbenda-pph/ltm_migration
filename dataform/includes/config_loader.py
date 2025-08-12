from google.cloud import storage
import json

def load_config():
    """Carga la configuración desde GCS"""
    client = storage.Client()
    bucket = client.bucket("ltm_migration")
    blob = bucket.blob("generate_dataform_config/latest.json")
    return json.loads(blob.download_as_string())