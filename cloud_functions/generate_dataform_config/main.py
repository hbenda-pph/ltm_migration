import json
from google.cloud import bigquery, storage
from datetime import datetime
import pytz

PROJECT_SOURCE = "tu-proyecto-metadata"  # Ajustar con tu proyecto
DATASET_NAME = "tu-dataset"             # Ajustar con tu dataset
TABLE_NAME = "companies"                # Tu tabla de control
BUCKET_NAME = "tu-bucket-configs"      # Bucket para guardar configs

def fetch_companies_to_replicate():
    """Consulta las compañías pendientes de replicación"""
    bq = bigquery.Client()
    query = f"""
        SELECT
            company_id,
            company_name,
            company_project_id,
            company_bigquery_status
        FROM `{PROJECT_SOURCE}.{DATASET_NAME}.{TABLE_NAME}`
        WHERE company_ltm_status = 0  # SOLO pendientes (PENDING)
        ORDER BY company_id
    """
    return list(bq.query(query))

def generate_dataform_config(companies):
    """Genera la configuración para Dataform"""
    return {
        "timestamp": datetime.now(pytz.UTC).isoformat(),
        "active_companies": [
            {
                "id": row.company_id,
                "name": row.company_name,
                "project": row.company_project_id,
                "status": "active" if row.company_bigquery_status else "inactive",
                "datasets": {
                    "raw": "raw_data",       # Ajustar según tu estructura
                    "analytics": "analytics" # Ajustar según tu estructura
                }
            } for row in companies
        ]
    }

def upload_config_to_gcs(config):
    """Sube la configuración a Google Cloud Storage"""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob("dataform_config/latest.json")
    blob.upload_from_string(json.dumps(config, indent=2))

def update_company_status(company_id, new_status):
    """Actualiza el estado en la tabla companies"""
    bq = bigquery.Client()
    query = f"""
        UPDATE `{PROJECT_SOURCE}.{DATASET_NAME}.{TABLE_NAME}`
        SET company_ltm_status = {new_status},
            last_replication_attempt = CURRENT_TIMESTAMP()
        WHERE company_id = '{company_id}'
    """
    bq.query(query).result()

def dataform_replication_handler(request):
    """Función principal HTTP"""
    try:
        # 1. Obtener compañías pendientes
        companies = fetch_companies_to_replicate()
        if not companies:
            return {"status": "skipped", "message": "No companies pending replication"}, 200
        
        # 2. Generar configuración
        config = generate_dataform_config(companies)
        
        # 3. Bloquear compañías (marcar como IN_PROGRESS)
        for company in config["active_companies"]:
            update_company_status(company["id"], 3)  # 3 = IN_PROGRESS
        
        # 4. Subir configuración a GCS
        upload_config_to_gcs(config)
        
        # 5. Actualizar estados a SUCCESS
        for company in config["active_companies"]:
            update_company_status(company["id"], 1)  # 1 = SUCCESS
        
        return {
            "status": "success",
            "companies_processed": len(companies),
            "config_path": f"gs://{BUCKET_NAME}/dataform_config/latest.json"
        }, 200
    
    except Exception as e:
        # En caso de error, actualizar estados
        if 'companies' in locals():
            for company in companies:
                update_company_status(company.company_id, 2)  # 2 = ERROR
        return {"status": "error", "message": str(e)}, 500