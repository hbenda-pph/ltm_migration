#!/usr/bin/env python3
"""
Script de configuración inicial del repositorio LTM Migration
"""

import sys
import os
from pathlib import Path

# Agregar el directorio padre al path para importar módulos
sys.path.append(str(Path(__file__).parent.parent.parent / "dataform" / "includes" / "config"))

from projects import ProjectManager

def test_connection():
    """Prueba la conexión con BigQuery y la tabla companies"""
    print("🔍 Probando conexión con BigQuery...")
    
    project_manager = ProjectManager()
    
    try:
        companies = project_manager.get_all_companies()
        print(f"✅ Conexión exitosa. Encontradas {len(companies)} compañías")
        
        # Mostrar compañías con estado 0 (pendientes)
        pending = project_manager.get_companies_to_replicate()
        print(f"📋 Compañías pendientes (estado 0): {len(pending)}")
        
        for company in pending:
            print(f"  - {company['company_name']} (ID: {company['company_id']})")
            print(f"    Project: {company['project_id']}")
            print(f"    Raw Dataset: {project_manager.get_raw_dataset_name(company['project_id'])}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error de conexión: {e}")
        return False

def test_raw_dataset_generation():
    """Prueba la generación de raw_dataset"""
    print("\n🧪 Probando generación de raw_dataset...")
    
    project_manager = ProjectManager()
    companies = project_manager.get_all_companies()
    
    if not companies:
        print("⚠️ No se encontraron compañías")
        return False
    
    print("📋 Ejemplos de generación:")
    for company in companies[:3]:  # Mostrar las primeras 3
        project_id = company['project_id']
        raw_dataset = project_manager.get_raw_dataset_name(project_id)
        print(f"  {company['company_name']}:")
        print(f"    Project ID: {project_id}")
        print(f"    Raw Dataset: {raw_dataset}")
    
    return True

def check_dataform_config():
    """Verifica la configuración de Dataform"""
    print("\n📋 Verificando configuración de Dataform...")
    
    dataform_path = Path(__file__).parent.parent.parent / "dataform" / "dataform.json"
    
    if not dataform_path.exists():
        print("❌ No se encontró dataform.json")
        return False
    
    print("✅ dataform.json encontrado")
    
    # Verificar que las vistas existen
    views_path = Path(__file__).parent.parent.parent / "dataform" / "definitions" / "staging" / "silver"
    
    if not views_path.exists():
        print("❌ No se encontró el directorio de vistas")
        return False
    
    views = list(views_path.glob("*.sql"))
    print(f"✅ Encontradas {len(views)} vistas SQL:")
    
    for view in views:
        print(f"  - {view.name}")
    
    return True

def main():
    """Función principal de configuración"""
    print("🚀 Configuración inicial del repositorio LTM Migration")
    print("=" * 60)
    
    # Prueba de conexión
    if not test_connection():
        print("\n❌ Falló la prueba de conexión. Verifica:")
        print("  1. Credenciales de BigQuery configuradas")
        print("  2. Tabla companies existe en platform-partners-qua.settings.companies")
        print("  3. Campo company_ltm_status agregado")
        return False
    
    # Prueba de generación de raw_dataset
    if not test_raw_dataset_generation():
        print("\n❌ Falló la prueba de raw_dataset")
        return False
    
    # Verificación de configuración de Dataform
    if not check_dataform_config():
        print("\n❌ Falló la verificación de Dataform")
        return False
    
    print("\n🎉 Configuración inicial completada exitosamente!")
    print("\n📋 Próximos pasos:")
    print("  1. Crear repositorio en GitHub")
    print("  2. Subir código al repositorio")
    print("  3. Configurar Dataform con el repositorio")
    print("  4. Probar despliegue con: make test-raw-dataset")
    print("  5. Crear funciones: make create-functions")
    print("  6. Probar despliegue: make deploy-prod")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 