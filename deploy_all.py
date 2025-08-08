#!/usr/bin/env python3
"""
Script simple para desplegar vistas Dataform a todos los proyectos de compañías.
Lee la tabla companies desde platform-partners-pro y ejecuta Dataform para cada proyecto.
"""

import subprocess
import json
import sys
from pathlib import Path
from google.cloud import bigquery

def get_companies_from_bigquery():
    """Obtiene solo los proyectos pendientes de replicación (estado 0)."""
    client = bigquery.Client(project="platform-partners-pro")
    
    query = """
    SELECT company_id
         , company_name
         , company_project_id
         , company_ltm_status
    FROM `platform-partners-pro.settings.companies`
    WHERE company_ltm_status = 0
    ORDER BY company_id
    """
    
    try:
        query_job = client.query(query)
        results = query_job.result()
        
        companies = []
        for row in results:
            companies.append({
                "company_id": row.company_id,
                "company_name": row.company_name,
                "company_project_id": row.company_project_id,
                "company_ltm_status": row.company_ltm_status
            })
        
        return companies
        
    except Exception as e:
        print(f"❌ Error obteniendo compañías desde BigQuery: {e}")
        return []

def update_dataform_config(project_id: str):
    """Actualiza dataform.json con el proyecto actual."""
    dataform_path = Path("dataform/dataform.json")
    
    with open(dataform_path, 'r') as f:
        config = json.load(f)
    
    config['defaultDatabase'] = project_id
    config['vars']['raw_dataset'] = f"servicetitan_{project_id.replace('-', '_')}"
    
    with open(dataform_path, 'w') as f:
        json.dump(config, f, indent=2)

def run_dataform():
    """Ejecuta Dataform compile y run."""
    dataform_dir = Path("dataform")
    
    try:
        print("  🔄 Compilando...")
        subprocess.run(["dataform", "compile"], cwd=dataform_dir, check=True, capture_output=True)
        
        print("  🚀 Ejecutando...")
        subprocess.run(["dataform", "run"], cwd=dataform_dir, check=True, capture_output=True)
        
        return True
    except subprocess.CalledProcessError as e:
        print(f"  ❌ Error ejecutando Dataform: {e}")
        return False

def main():
    """Función principal."""
    print("🚀 Iniciando despliegue a proyectos pendientes...")
    print("=" * 60)
    
    # Obtener compañías pendientes (estado 0)
    companies = get_companies_from_bigquery()
    
    if not companies:
        print("ℹ️ No hay compañías pendientes de replicación (estado 0)")
        print("💡 Para agregar una compañía a la cola, actualiza su company_ltm_status a 0")
        return
    
    print(f"📋 Encontradas {len(companies)} compañías pendientes:")
    for company in companies:
        print(f"  ⏳ {company['company_name']} ({company['company_project_id']})")
    
    print("\n🔄 Iniciando despliegue...")
    
    success_count = 0
    error_count = 0
    
    for company in companies:
        project_id = company['company_project_id']
        company_name = company['company_name']
        
        print(f"\n--- Procesando {company_name} ({project_id}) ---")
        
        # Actualizar configuración
        update_dataform_config(project_id)
        
        # Ejecutar Dataform
        if run_dataform():
            print(f"  ✅ Éxito en {project_id}")
            success_count += 1
        else:
            print(f"  ❌ Error en {project_id}")
            error_count += 1
    
    print(f"\n📊 Resumen de despliegue:")
    print(f"  ✅ Exitosos: {success_count}")
    print(f"  ❌ Fallidos: {error_count}")
    print(f"  📊 Total: {len(companies)}")
    
    if success_count > 0:
        print(f"\n💡 Para marcar como exitoso, actualiza company_ltm_status a 1")
        print(f"💡 Para marcar como error, actualiza company_ltm_status a 2")

if __name__ == "__main__":
    main() 