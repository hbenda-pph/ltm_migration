import json
from google.cloud import bigquery, storage
from datetime import datetime
import pytz

PROJECT_SOURCE = "constant-height-455614-i0"    
DATASET_NAME = "settings"                              
TABLE_NAME = "companies"                               
BUCKET_NAME = "ltm_migration" 

def fetch_companies_to_replicate():
    """Consulta las compañías pendientes de replicación"""
    bq = bigquery.Client()
    query = f"""
        SELECT company_id
             , company_name
             , company_project_id
             , company_ltm_status
        FROM `{PROJECT_SOURCE}.{DATASET_NAME}.{TABLE_NAME}`
       WHERE company_ltm_status = CAST(0 AS INT64)
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
                "ltm_status": row.company_ltm_status,
                "datasets": {
                    "raw": "servicetitan_"+row.company_project_id.replace("-", "_"),
                    "bronze": "bronze"
                }
            } for row in companies
        ]
    }

def upload_config_to_gcs(config):
    try:
        client = storage.Client()
        # Verifica que el bucket existe
        bucket = client.get_bucket(BUCKET_NAME)
        print(f"Bucket encontrado: {BUCKET_NAME}")
        
        blob = bucket.blob("dataform_config/latest.json")
        blob.upload_from_string(json.dumps(config, indent=2))
        print("Configuración subida exitosamente")
    except Exception as e:
        print(f"Error subiendo a GCS: {str(e)}")
        raise

def update_company_status(company_id: int, new_status: int):
    """Actualiza el estado en la tabla companies"""
    bq = bigquery.Client()
    query = f"""
        UPDATE `{PROJECT_SOURCE}.{DATASET_NAME}.{TABLE_NAME}`
        SET company_ltm_status = {new_status}
        WHERE company_id = {company_id}
    """
    bq.query(query).result()

def trigger_dataform_execution(companies=None):

    from googleapiclient.discovery import build
    from google.auth import default
    
    credentials, _ = default()
    service = build('dataform', 'v1beta1', credentials=credentials)
   
    # 2. Configuración de la ejecución
    project_id = "constant-height-455614-i0"  # Reemplaza con tu Project ID
    location = "us-central1"    # Ajusta la región si es diferente
    repository_id = "ltm_migration"
    workspace_id = "ltm_migration_wrkspc"  # Usualmente "default"

    try:
        response = service.projects().locations().repositories().workflowInvocations().create(
            parent=f"projects/{project_id}/locations/{location}/repositories/{repository_id}",
            workflowInvocation={
                "compilationResult": {
                    "gitCommitish": "main",
                    "workspace": (
                        f"projects/{project_id}/locations/{location}/"
                        f"repositories/{repository_id}/workspaces/{workspace_id}"
                    )
                },
                "invocationConfig": {
                    "includedTags": ["multi-tenant"],
                    "transitiveDependenciesIncluded": True
                }
            }
        ).execute()
        
        print(f"Ejecución iniciada: {response['name']}")
        return True
    except Exception as e:
        print(f"Error al ejecutar Dataform: {str(e)}")
        return False

def dataform_replication_handler(request):
    print("Inicio de ejecución")  # Log inicial
    
    try:
        print("Obteniendo compañías...")
        companies = fetch_companies_to_replicate()
        print(f"Compañías encontradas: {len(companies)}")
        
        if not companies:
            print("No hay compañías pendientes")
            return {"status": "skipped"}, 200
        
        print("Generando configuración...")
        config = generate_dataform_config(companies)
        
        print("Actualizando estados a IN_PROGRESS...")
        for company in config["active_companies"]:
            update_company_status(company["id"], 3)
            print(f"Actualizado {company['id']}")
        
        print("Subiendo configuración a GCS...")
        upload_config_to_gcs(config)
        
        print("Actualizando estados a SUCCESS...")
        for company in config["active_companies"]:
            update_company_status(company["id"], 1)

        trigger_dataform_execution(companies)  
        
        print("Proceso completado")
        return {
            "status": "success",
            "companies_processed": len(companies)
        }, 200
    
    except Exception as e:
        print(f"ERROR: {str(e)}")
        if 'companies' in locals():
            print("Intentando actualizar estados a ERROR...")
            for company in companies:
                try:
                    update_company_status(company.company_id, 2)
                except Exception as update_error:
                    print(f"Error actualizando estado: {str(update_error)}")
        
        # Registra el stack trace completo
        import traceback
        traceback.print_exc()
        
        return {"status": "error", "message": str(e)}, 500