#!/usr/bin/env python3
"""
Script simple para desplegar vistas Dataform a todos los proyectos de compañías.
Lee la tabla companies desde platform-partners-pro y ejecuta Dataform para cada proyecto.
Ejecución ON-DEMAND para despliegues de nuevas vistas y estructuras.
"""

import subprocess
import json
import sys
import datetime
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
        result = subprocess.run(["dataform", "compile"], cwd=dataform_dir, check=True, capture_output=True, text=True)
        print(f"    ✅ Compilación exitosa")
        
        print("  🚀 Ejecutando...")
        result = subprocess.run(["dataform", "run"], cwd=dataform_dir, check=True, capture_output=True, text=True)
        print(f"    ✅ Ejecución exitosa")
        
        return True
    except subprocess.CalledProcessError as e:
        print(f"  ❌ Error ejecutando Dataform: {e}")
        if e.stdout:
            print(f"    STDOUT: {e.stdout}")
        if e.stderr:
            print(f"    STDERR: {e.stderr}")
        return False

def main():
    """Función principal."""
    start_time = datetime.datetime.now()
    print("🚀 LTM Migration - Despliegue ON-DEMAND")
    print("=" * 60)
    print(f"⏰ Inicio: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🎯 Objetivo: Desplegar vistas y funciones a proyectos pendientes")
    print("=" * 60)
    
    # Obtener compañías pendientes (estado 0)
    companies = get_companies_from_bigquery()
    
    if not companies:
        print("ℹ️ No hay compañías pendientes de replicación (estado 0)")
        print("💡 Para agregar una compañía a la cola:")
        print("   UPDATE `platform-partners-pro.settings.companies`")
        print("   SET company_ltm_status = 0")
        print("   WHERE company_id = 'TU_COMPAÑIA'")
        return
    
    print(f"📋 Encontradas {len(companies)} compañías pendientes:")
    for company in companies:
        print(f"  ⏳ {company['company_name']} ({company['company_project_id']})")
    
    print(f"\n🔄 Iniciando despliegue...")
    
    success_count = 0
    error_count = 0
    errors = []
    
    for i, company in enumerate(companies, 1):
        project_id = company['company_project_id']
        company_name = company['company_name']
        
        print(f"\n--- [{i}/{len(companies)}] Procesando {company_name} ({project_id}) ---")
        
        # Actualizar configuración
        update_dataform_config(project_id)
        
        # Ejecutar Dataform
        if run_dataform():
            print(f"  ✅ Éxito en {project_id}")
            success_count += 1
        else:
            print(f"  ❌ Error en {project_id}")
            error_count += 1
            errors.append(f"{company_name} ({project_id})")
    
    end_time = datetime.datetime.now()
    duration = end_time - start_time
    
    print(f"\n📊 Resumen de despliegue:")
    print(f"  ✅ Exitosos: {success_count}")
    print(f"  ❌ Fallidos: {error_count}")
    print(f"  📊 Total: {len(companies)}")
    print(f"  ⏱️ Duración: {duration}")
    
    if errors:
        print(f"\n❌ Errores encontrados:")
        for error in errors:
            print(f"  - {error}")
    
    if success_count > 0:
        print(f"\n💡 Para marcar como exitoso:")
        print(f"   UPDATE `platform-partners-pro.settings.companies`")
        print(f"   SET company_ltm_status = 1")
        print(f"   WHERE company_ltm_status = 0")
        
    if error_count > 0:
        print(f"\n💡 Para marcar como error:")
        print(f"   UPDATE `platform-partners-pro.settings.companies`")
        print(f"   SET company_ltm_status = 2")
        print(f"   WHERE company_ltm_status = 0")

if __name__ == "__main__":
    main() 