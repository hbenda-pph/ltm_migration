# Makefile para LTM Migration
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

# Comandos de despliegue por ambiente
deploy: ## Desplegar a todos los proyectos
	$(PYTHON) scripts/deployment/deploy_to_projects.py --all

deploy-dev: ## Desplegar solo a desarrollo (pruebas unitarias)
	$(PYTHON) scripts/deployment/deploy_to_projects.py --environment dev

deploy-qa: ## Desplegar solo a QA (pruebas funcionales)
	$(PYTHON) scripts/deployment/deploy_to_projects.py --environment qa

deploy-prod: ## Desplegar solo compañías pendientes en producción
	$(PYTHON) scripts/deployment/deploy_to_projects.py --pending

deploy-retry: ## Reintentar compañías con errores en producción
	$(PYTHON) scripts/deployment/deploy_to_projects.py --retry

# Comandos de gestión de estados
status: ## Mostrar estado de replicaciones
	$(PYTHON) scripts/deployment/deploy_to_projects.py --status

# Comandos de configuración de tabla
add-ltm-columns: ## Agregar columnas LTM a la tabla companies
	$(PYTHON) scripts/setup/create_companies_table.py --add-columns

companies-info: ## Mostrar información de tabla companies
	$(PYTHON) scripts/setup/create_companies_table.py --info

update-sample-statuses: ## Actualizar estados de ejemplo (solo testing)
	$(PYTHON) scripts/setup/create_companies_table.py --update-samples

# Comandos de funciones personalizadas
create-functions: ## Crear funciones personalizadas en BigQuery
	$(PYTHON) scripts/setup/create_functions.py

# Comandos de prueba
test-raw-dataset: ## Probar generación dinámica de raw_dataset
	$(PYTHON) scripts/setup/test_raw_dataset.py

setup-repo: ## Configurar repositorio inicial
	$(PYTHON) scripts/setup/setup_repository.py

# Comandos de calidad
quality-check: ## Verificar calidad de datos
	$(PYTHON) scripts/monitoring/quality_checks.py --project platform-partners-qua --dataset silver --all-tables

# Comandos de limpieza
clean: ## Limpiar archivos temporales
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	rm -rf .pytest_cache/

# Comando de ayuda
help: ## Mostrar esta ayuda
	@echo "Comandos disponibles:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# Comandos de ejemplo por ambiente
example-dev: ## Ejemplo de flujo para desarrollo
	@echo "🚀 Flujo de desarrollo:"
	@echo "1. Compilar: make compile"
	@echo "2. Probar en dev: make deploy-dev"
	@echo "3. Verificar calidad: make quality-check"

example-qa: ## Ejemplo de flujo para QA
	@echo "🚀 Flujo de QA:"
	@echo "1. Desplegar a QA: make deploy-qa"
	@echo "2. Verificar calidad: make quality-check"
	@echo "3. Ver estado: make status"

example-prod: ## Ejemplo de flujo para producción
	@echo "🚀 Flujo de producción:"
	@echo "1. Ver estado: make status"
	@echo "2. Desplegar pendientes: make deploy-prod"
	@echo "3. Verificar calidad: make quality-check"
	@echo "4. Reintentar errores si es necesario: make deploy-retry" 