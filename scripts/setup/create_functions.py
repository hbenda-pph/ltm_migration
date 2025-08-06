#!/usr/bin/env python3
"""
Script para crear funciones personalizadas en BigQuery
"""

import os
import sys
from google.cloud import bigquery
from pathlib import Path

# Agregar el directorio padre al path para importar módulos
sys.path.append(str(Path(__file__).parent.parent))

from includes.config.projects import ProjectManager

def create_convert_utc_localtz_function(project_id: str, dataset_id: str):
    """
    Crea la función convert_utc_localtz en el dataset especificado
    """
    client = bigquery.Client(project=project_id)
    
    # Leer el script SQL
    script_path = Path(__file__).parent / "create_convert_utc_localtz_function.sql"
    
    with open(script_path, 'r') as f:
        sql_script = f.read()
    
    # Reemplazar variables en el script
    sql_script = sql_script.replace('${dataform.projectId}', project_id)
    sql_script = sql_script.replace('${dataform.vars.bronze_dataset}', dataset_id)
    
    try:
        # Ejecutar el script
        job = client.query(sql_script)
        job.result()  # Esperar a que termine
        
        print(f"✅ Función convert_utc_localtz creada exitosamente en {project_id}.{dataset_id}")
        return True
        
    except Exception as e:
        print(f"❌ Error creando la función: {e}")
        return False

def main():
    """
    Función principal para crear funciones en todos los proyectos
    """
    project_manager = ProjectManager()
    
    print("🔧 Creando función convert_utc_localtz en todos los proyectos...")
    
    # Obtener todos los proyectos
    companies = project_manager.get_all_companies()
    
    success_count = 0
    total_count = len(companies)
    
    for company in companies:
        project_id = company['project_id']
        dataset_id = 'bronze'  # La función se crea en el dataset bronze
        
        print(f"📋 Procesando {company['company_name']} ({project_id})...")
        
        if create_convert_utc_localtz_function(project_id, dataset_id):
            success_count += 1
    
    print(f"\n📊 Resumen:")
    print(f"   ✅ Exitosos: {success_count}")
    print(f"   ❌ Fallidos: {total_count - success_count}")
    print(f"   📈 Total: {total_count}")

if __name__ == "__main__":
    main() 