#!/usr/bin/env python3
"""
Script de despliegue para múltiples proyectos BigQuery usando Dataform.
Ahora gestiona estados de replicación desde la tabla companies.
"""

import subprocess
import json
import sys
import os
from pathlib import Path
from typing import Optional, List, Dict
from datetime import datetime

# Agregar el directorio raíz al path para importar config
sys.path.append(str(Path(__file__).parent.parent.parent / "dataform" / "includes" / "config"))
from projects import (
    get_project_config, 
    get_companies_to_replicate, 
    get_all_companies,
    update_company_status,
    project_manager
)

# Estados de replicación
REPLICATION_STATUS = {
    -1: "NO_REPLICATE",      # No replicar
    0: "PENDING",            # Nunca ha replicado y lo requiere
    1: "SUCCESS",            # Replicado con éxito
    2: "ERROR",              # Replicado con error
    3: "IN_PROGRESS",        # Replicación en progreso
    4: "ROLLBACK",           # Rollback requerido
    5: "DEPRECATED"          # Compañía deprecada
}

def update_dataform_config(project_id: str) -> None:
    """
    Actualiza el archivo dataform.json con el proyecto actual y raw_dataset dinámico.
    
    Args:
        project_id: ID del proyecto de BigQuery
    """
    dataform_path = Path(__file__).parent.parent.parent / "dataform" / "dataform.json"
    
    with open(dataform_path, 'r') as f:
        config = json.load(f)
    
    config['defaultDatabase'] = project_id
    
    # Generar raw_dataset dinámicamente
    raw_dataset = project_manager.get_raw_dataset_name(project_id)
    config['vars']['raw_dataset'] = raw_dataset
    
    with open(dataform_path, 'w') as f:
        json.dump(config, f, indent=2)

def run_command(command: str, cwd: Optional[str] = None) -> None:
    """
    Ejecuta un comando del sistema.
    
    Args:
        command: Comando a ejecutar
        cwd: Directorio de trabajo (opcional)
    """
    try:
        subprocess.run(command, shell=True, check=True, cwd=cwd, capture_output=False)
    except subprocess.CalledProcessError as e:
        print(f"❌ Error ejecutando comando: {command}")
        print(f"Error: {e}")
        raise

def deploy_to_project(environment: str, company_id: str = None) -> bool:
    """
    Despliega a un proyecto específico con gestión de estados.
    
    Args:
        environment: Ambiente ('dev', 'qa', 'prod')
        company_id: ID de la compañía (requerido para 'prod')
    
    Returns:
        True si el despliegue fue exitoso
    """
    try:
        config = get_project_config(environment, company_id)
        project_id = config['projectId']
        
        print(f"🚀 Desplegando a {environment}{f' - {company_id}' if company_id else ''} ({project_id})")
        
        # Si es producción, actualizar estado a "en progreso"
        if environment == 'prod' and company_id:
            update_company_status(company_id, 3)  # IN_PROGRESS
        
        # Actualizar dataform.json con el proyecto actual
        update_dataform_config(project_id)
        
        # Cambiar al directorio dataform
        dataform_dir = Path(__file__).parent.parent.parent / "dataform"
        
        # Compilar y ejecutar Dataform
        run_command("dataform compile", cwd=dataform_dir)
        run_command("dataform run", cwd=dataform_dir)
        
        print(f"✅ Despliegue exitoso a {project_id}")
        
        # Si es producción, actualizar estado a "éxito"
        if environment == 'prod' and company_id:
            update_company_status(company_id, 1)  # SUCCESS
        
        return True
        
    except Exception as error:
        print(f"❌ Error desplegando a {environment}{f' - {company_id}' if company_id else ''}: {error}")
        
        # Si es producción, actualizar estado a "error"
        if environment == 'prod' and company_id:
            update_company_status(company_id, 2, str(error))  # ERROR
        
        return False

def deploy_all() -> None:
    """Despliega a todos los proyectos configurados."""
    print("🚀 Iniciando despliegue a todos los proyectos...\n")
    
    # Despliegue a desarrollo y QA
    for env in ['dev', 'qa']:
        deploy_to_project(env)
    
    # Despliegue a producción solo para compañías pendientes
    companies_to_replicate = get_companies_to_replicate()
    
    if not companies_to_replicate:
        print("ℹ️ No hay compañías pendientes de replicación")
    else:
        print(f"📋 Encontradas {len(companies_to_replicate)} compañías pendientes de replicación")
        
        for company in companies_to_replicate:
            success = deploy_to_project('prod', company['company_id'])
            if not success:
                print(f"⚠️ Falló replicación para {company['company_name']}")
    
    print("\n🎉 Despliegue completado!")

def deploy_pending_companies() -> None:
    """Despliega solo las compañías pendientes de replicación."""
    print("🚀 Desplegando compañías pendientes...\n")
    
    companies = get_companies_to_replicate()
    
    if not companies:
        print("ℹ️ No hay compañías pendientes de replicación")
        return
    
    print(f"📋 Encontradas {len(companies)} compañías pendientes:")
    for company in companies:
        print(f"  - {company['company_name']} ({company['company_id']})")
    
    print("\n🔄 Iniciando replicaciones...")
    
    success_count = 0
    error_count = 0
    
    for company in companies:
        print(f"\n--- Replicando {company['company_name']} ---")
        success = deploy_to_project('prod', company['company_id'])
        
        if success:
            success_count += 1
        else:
            error_count += 1
    
    print(f"\n📊 Resumen de replicaciones:")
    print(f"  ✅ Exitosas: {success_count}")
    print(f"  ❌ Fallidas: {error_count}")
    print(f"  📊 Total: {len(companies)}")

def deploy_single(environment: str, company_id: str = None) -> None:
    """
    Despliega a un solo proyecto.
    
    Args:
        environment: Ambiente
        company_id: ID de la compañía (opcional)
    """
    success = deploy_to_project(environment, company_id)
    if success:
        print("✅ Despliegue completado exitosamente")
    else:
        print("❌ Despliegue falló")
        sys.exit(1)

def show_replication_status() -> None:
    """Muestra el estado actual de las replicaciones."""
    print("📊 Estado de Replicaciones\n")
    
    # Obtener resumen
    summary = project_manager.get_replication_summary()
    
    if not summary:
        print("❌ No se pudo obtener el resumen de estados")
        return
    
    print("📈 Resumen por Estado:")
    for status_name, count in summary.items():
        print(f"  {status_name}: {count} compañías")
    
    print("\n📋 Compañías por Estado:")
    
    # Mostrar compañías pendientes
    pending = get_companies_to_replicate()
    if pending:
        print(f"\n🔄 Pendientes (0):")
        for company in pending:
            print(f"  - {company['company_name']} ({company['company_id']})")
    
    # Mostrar compañías con errores
    errors = project_manager.get_companies_with_errors()
    if errors:
        print(f"\n❌ Con Errores (2):")
        for company in errors:
            print(f"  - {company['company_name']} ({company['company_id']})")
    
    # Mostrar todas las compañías
    all_companies = get_all_companies()
    if all_companies:
        print(f"\n📋 Todas las Compañías:")
        for company in all_companies:
            status = REPLICATION_STATUS.get(company['company_ltm_status'], f"UNKNOWN_{company['company_ltm_status']}")
            print(f"  - {company['company_name']} ({company['company_id']}) - {status}")

def retry_failed_companies() -> None:
    """Reintenta replicación para compañías con errores."""
    print("🔄 Reintentando replicación para compañías con errores...\n")
    
    failed_companies = project_manager.get_companies_with_errors()
    
    if not failed_companies:
        print("ℹ️ No hay compañías con errores para reintentar")
        return
    
    print(f"📋 Encontradas {len(failed_companies)} compañías con errores:")
    for company in failed_companies:
        print(f"  - {company['company_name']} ({company['company_id']})")
    
    print("\n🔄 Iniciando reintentos...")
    
    success_count = 0
    error_count = 0
    
    for company in failed_companies:
        print(f"\n--- Reintentando {company['company_name']} ---")
        success = deploy_to_project('prod', company['company_id'])
        
        if success:
            success_count += 1
        else:
            error_count += 1
    
    print(f"\n📊 Resumen de reintentos:")
    print(f"  ✅ Exitosos: {success_count}")
    print(f"  ❌ Fallidos: {error_count}")
    print(f"  📊 Total: {len(failed_companies)}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Desplegar vistas Dataform a proyectos BigQuery")
    parser.add_argument("--environment", "-e", choices=['dev', 'qa', 'prod'], help="Ambiente a desplegar")
    parser.add_argument("--company", "-c", help="ID de la compañía (requerido para producción)")
    parser.add_argument("--all", action="store_true", help="Desplegar a todos los proyectos")
    parser.add_argument("--pending", action="store_true", help="Desplegar solo compañías pendientes")
    parser.add_argument("--retry", action="store_true", help="Reintentar compañías con errores")
    parser.add_argument("--status", action="store_true", help="Mostrar estado de replicaciones")
    
    args = parser.parse_args()
    
    if args.status:
        show_replication_status()
    elif args.retry:
        retry_failed_companies()
    elif args.pending:
        deploy_pending_companies()
    elif args.all:
        deploy_all()
    elif args.environment:
        deploy_single(args.environment, args.company)
    else:
        print("❌ Debes especificar una opción válida")
        print("Opciones disponibles:")
        print("  --all: Desplegar a todos los proyectos")
        print("  --pending: Desplegar solo compañías pendientes")
        print("  --retry: Reintentar compañías con errores")
        print("  --status: Mostrar estado de replicaciones")
        print("  --environment: Desplegar a ambiente específico")
        sys.exit(1) 