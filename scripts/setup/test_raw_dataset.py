#!/usr/bin/env python3
"""
Script de prueba para verificar la generación dinámica del raw_dataset
"""

import sys
from pathlib import Path

# Agregar el directorio padre al path para importar módulos
sys.path.append(str(Path(__file__).parent.parent.parent / "dataform" / "includes" / "config"))

from projects import ProjectManager

def test_raw_dataset_generation():
    """
    Prueba la generación del raw_dataset para diferentes project_ids
    """
    project_manager = ProjectManager()
    
    # Casos de prueba
    test_cases = [
        ("shape-mhs-1", "servicetitan_shape_mhs_1"),
        ("company-abc-123", "servicetitan_company_abc_123"),
        ("test-project", "servicetitan_test_project"),
        ("simple", "servicetitan_simple"),
        ("multiple-dashes-here", "servicetitan_multiple_dashes_here")
    ]
    
    print("🧪 Probando generación de raw_dataset dinámico...")
    print("📋 Regla: Los datasets no pueden tener guiones, los proyectos sí")
    print("=" * 60)
    
    all_passed = True
    
    for project_id, expected in test_cases:
        result = project_manager.get_raw_dataset_name(project_id)
        status = "✅" if result == expected else "❌"
        
        print(f"{status} Project: {project_id} → Dataset: {result}")
        
        if result != expected:
            print(f"   Esperado: {expected}")
            all_passed = False
    
    print("=" * 60)
    
    if all_passed:
        print("🎉 Todas las pruebas pasaron!")
    else:
        print("❌ Algunas pruebas fallaron!")
    
    return all_passed

def test_with_real_companies():
    """
    Prueba con compañías reales de la tabla companies
    """
    project_manager = ProjectManager()
    
    print("\n🏢 Probando con compañías reales...")
    print("=" * 60)
    
    companies = project_manager.get_all_companies()
    
    if not companies:
        print("⚠️  No se encontraron compañías en la tabla companies")
        return
    
    for company in companies[:5]:  # Mostrar solo las primeras 5
        project_id = company.get('project_id', 'N/A')
        company_name = company.get('company_name', 'N/A')
        
        if project_id != 'N/A':
            raw_dataset = project_manager.get_raw_dataset_name(project_id)
            print(f"📋 {company_name}")
            print(f"   Project ID: {project_id}")
            print(f"   Raw Dataset: {raw_dataset}")
            print(f"   Nota: Project mantiene guiones, Dataset los convierte a guiones bajos")
            print()

if __name__ == "__main__":
    print("🚀 Iniciando pruebas de raw_dataset dinámico...")
    
    # Prueba con casos de prueba
    test_passed = test_raw_dataset_generation()
    
    # Prueba con compañías reales
    test_with_real_companies()
    
    if test_passed:
        print("\n✅ Todas las pruebas completadas exitosamente!")
    else:
        print("\n❌ Algunas pruebas fallaron!")
        sys.exit(1) 