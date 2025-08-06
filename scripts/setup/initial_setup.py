#!/usr/bin/env python3
"""
Script de configuración inicial para el proyecto LTM Migration con Dataform.
"""

import os
import json
from pathlib import Path
from typing import Dict, Any

def ensure_directory_exists(dir_path: str) -> None:
    """Crea un directorio si no existe."""
    path = Path(dir_path)
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
        print(f"✅ Creado directorio: {dir_path}")

def create_file_if_not_exists(file_path: str, content: str) -> None:
    """Crea un archivo si no existe."""
    path = Path(file_path)
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"✅ Creado archivo: {file_path}")

def main():
    """Función principal de configuración."""
    print("🚀 Configurando proyecto LTM Migration con Dataform...\n")
    
    # Crear estructura de directorios
    directories = [
        'dataform/definitions/sources/bronze',
        'dataform/definitions/staging/silver',
        'dataform/definitions/marts/gold',
        'dataform/includes/config',
        'scripts/deployment',
        'scripts/monitoring',
        'docs',
        'tests'
    ]
    
    for dir_path in directories:
        ensure_directory_exists(dir_path)
    
    # Archivos de configuración adicionales
    additional_files = [
        {
            "path": "dataform/includes/config/constants.sql",
            "content": """-- Constantes SQL para el proyecto
-- Estas constantes se pueden usar en todas las vistas

-- Configuración de fechas
DECLARE DEFAULT_DATE_FORMAT STRING DEFAULT '%Y-%m-%d';
DECLARE DEFAULT_TIMESTAMP_FORMAT STRING DEFAULT '%Y-%m-%d %H:%M:%S';

-- Configuración de validaciones
DECLARE MIN_RECORD_COUNT INT64 DEFAULT 1;
DECLARE MAX_RECORD_COUNT INT64 DEFAULT 1000000;

-- Configuración de auditoría
DECLARE AUDIT_COLUMNS STRING DEFAULT 'silver_processed_at, source_view';
"""
        },
        {
            "path": ".env.example",
            "content": """# Configuración de entorno para LTM Migration
# Copiar este archivo a .env y configurar los valores

# Google Cloud
GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account.json
GOOGLE_CLOUD_PROJECT=your-project-id

# Dataform
DATAFORM_WAREHOUSE=bigquery
DATAFORM_DEFAULT_SCHEMA=dataform

# Configuración de proyectos
DEV_PROJECT_ID=ltm-dev-project
QA_PROJECT_ID=ltm-qa-project
PROD_PROJECT_ID=ltm-prod-project

# Configuración de logging
LOG_LEVEL=INFO
"""
        },
        {
            "path": "Makefile",
            "content": """# Makefile para LTM Migration
.PHONY: setup install test compile run deploy clean help

# Configuración
PYTHON = python3
PIP = pip3

# Comandos principales
setup: ## Configurar el proyecto inicialmente
	$(PYTHON) scripts/setup/initial_setup.py

install: ## Instalar dependencias
	$(PIP) install -r requirements.txt

test: ## Ejecutar tests
	pytest tests/ -v

compile: ## Compilar Dataform
	cd dataform && dataform compile

run: ## Ejecutar Dataform
	cd dataform && dataform run

deploy: ## Desplegar a todos los proyectos
	$(PYTHON) scripts/deployment/deploy_to_projects.py --all

deploy-dev: ## Desplegar solo a desarrollo
	$(PYTHON) scripts/deployment/deploy_to_projects.py --environment dev

deploy-qa: ## Desplegar solo a QA
	$(PYTHON) scripts/deployment/deploy_to_projects.py --environment qa

quality-check: ## Verificar calidad de datos
	$(PYTHON) scripts/monitoring/quality_checks.py --project $(GOOGLE_CLOUD_PROJECT) --dataset silver --all-tables

clean: ## Limpiar archivos temporales
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	rm -rf .pytest_cache/

help: ## Mostrar esta ayuda
	@echo "Comandos disponibles:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
"""
        },
        {
            "path": "README.md",
            "content": """# LTM Migration - Dataform Project

## Descripción del Proyecto

Este proyecto utiliza Dataform para gestionar la migración de vistas SQL a múltiples proyectos de BigQuery siguiendo una arquitectura de medalla (Bronze, Silver, Gold).

## Arquitectura

### Capas de Datos
- **Bronze**: Datos raw sin procesar
- **Silver**: Datos procesados y validados (3 vistas piloto)
- **Gold**: Datos agregados para consumo final

### Proyectos Soportados
- **Desarrollo**: `ltm-dev-project`
- **Calidad**: `ltm-qa-project` 
- **Producción**: Múltiples proyectos por compañía

## Configuración Rápida

### 1. Instalar Dependencias
```bash
pip install -r requirements.txt
```

### 2. Configurar Variables de Entorno
```bash
cp .env.example .env
# Editar .env con tus configuraciones
```

### 3. Configurar Proyectos
Editar `dataform/includes/config/projects.py` con los IDs de tus proyectos.

### 4. Configurar Credenciales
```bash
# Autenticación con Google Cloud
gcloud auth application-default login

# O usar service account
export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account.json"
```

## Uso

### Comandos Principales
```bash
# Configuración inicial
make setup

# Instalar dependencias
make install

# Compilar Dataform
make compile

# Ejecutar Dataform
make run

# Desplegar a todos los proyectos
make deploy

# Verificar calidad de datos
make quality-check
```

### Despliegue Manual
```bash
# Desplegar a todos los proyectos
python scripts/deployment/deploy_to_projects.py --all

# Desplegar a proyecto específico
python scripts/deployment/deploy_to_projects.py --environment dev

# Desplegar a producción por compañía
python scripts/deployment/deploy_to_projects.py --environment prod --company company1
```

### Verificación de Calidad
```bash
# Verificar tabla específica
python scripts/monitoring/quality_checks.py --project your-project --dataset silver --table view1

# Verificar todas las tablas
python scripts/monitoring/quality_checks.py --project your-project --dataset silver --all-tables
```

## Estructura del Proyecto

```
ltm_migration/
├── dataform/
│   ├── definitions/
│   │   ├── sources/          # Fuentes de datos (Bronze)
│   │   ├── staging/          # Vistas Silver
│   │   └── marts/           # Vistas Gold
│   ├── includes/
│   │   └── config/          # Configuración de proyectos
│   └── dataform.json        # Configuración principal
├── scripts/
│   ├── deployment/          # Scripts de despliegue
│   ├── monitoring/          # Scripts de monitoreo
│   └── setup/              # Scripts de configuración
├── tests/                  # Tests unitarios
├── docs/                   # Documentación
├── requirements.txt         # Dependencias Python
└── Makefile               # Comandos automatizados
```

## Vistas Silver Implementadas

### 1. Vista Silver 1 - Datos Procesados
- **Propósito**: Limpieza y validación de datos
- **Entrada**: Datos de bronze
- **Salida**: Datos limpios con validaciones

### 2. Vista Silver 2 - Agregaciones
- **Propósito**: Métricas por compañía
- **Entrada**: Datos de bronze
- **Salida**: Agregaciones y porcentajes

### 3. Vista Silver 3 - Datos Enriquecidos
- **Propósito**: Enriquecimiento y segmentación
- **Entrada**: Datos de bronze
- **Salida**: Datos con categorizaciones

## Flujo de Trabajo

### 1. Desarrollo
1. Crear/modificar vistas en `dataform/definitions/`
2. Probar localmente con `make compile`
3. Commit y push a GitHub

### 2. Despliegue
1. Ejecutar `make deploy`
2. Verificar vistas en BigQuery
3. Monitorear logs y métricas

### 3. Monitoreo
- Revisar logs de ejecución
- Verificar calidad de datos con `make quality-check`
- Monitorear costos de BigQuery

## Troubleshooting

### Errores Comunes
1. **Credenciales**: Verificar autenticación de Google Cloud
2. **Permisos**: Asegurar acceso a proyectos BigQuery
3. **Configuración**: Verificar IDs de proyectos en config

### Logs
- Dataform logs: `dataform logs`
- BigQuery logs: Console de Google Cloud
- Deployment logs: Salida de scripts

## Contribución

1. Crear branch para nueva funcionalidad
2. Implementar cambios en Dataform
3. Probar en ambiente de desarrollo
4. Crear Pull Request
5. Revisar y aprobar cambios
6. Desplegar a producción

## Contacto

Para soporte técnico o preguntas sobre el proyecto, contactar al equipo de datos.
"""
        }
    ]
    
    for file_info in additional_files:
        create_file_if_not_exists(file_info["path"], file_info["content"])
    
    print("\n📋 Próximos pasos:")
    print("1. Configurar IDs de proyectos en dataform/includes/config/projects.py")
    print("2. Instalar dependencias: pip install -r requirements.txt")
    print("3. Configurar credenciales de Google Cloud")
    print("4. Copiar .env.example a .env y configurar variables")
    print("5. Probar configuración: make compile")
    print("6. Ejecutar despliegue: make deploy")
    
    print("\n🎉 Configuración inicial completada!")

if __name__ == "__main__":
    main() 