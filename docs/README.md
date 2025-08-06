# LTM Migration - Dataform Project

## Descripción del Proyecto

Este proyecto implementa la replicación de vistas SQL aprobadas desde un proyecto piloto de BigQuery hacia múltiples proyectos de diferentes compañías, utilizando Dataform con una arquitectura de medalla (bronze, silver, gold).

## Arquitectura

### Estructura de Datos
- **Bronze**: Datos raw/brutos
- **Silver**: Datos procesados y validados (vistas actuales)
- **Gold**: Datos agregados y finales (futuro)

### Ambientes
- **Development**: Pruebas unitarias y desarrollo
- **QA**: Pruebas funcionales
- **Production**: Ambiente final

## Configuración Inicial

### 1. Instalar Dependencias
```bash
make install
```

### 2. Configurar Tabla de Compañías
```bash
# Agregar columnas LTM a la tabla existente
make add-ltm-columns

# Ver información de la tabla
make companies-info
```

### 3. Crear Funciones Personalizadas
```bash
# Crear función convert_utc_localtz en todos los proyectos
make create-functions
```

## Vistas Implementadas

### 1. vw_new_customer_list
- **Descripción**: Lista de nuevos clientes con información de campañas y trabajos
- **Dataset**: Silver
- **Variables**: `${dataform.vars.new_customer_list_view}`

### 2. vw_sold_estimates
- **Descripción**: Estimaciones vendidas con información de clientes y técnicos
- **Dataset**: Silver
- **Variables**: `${dataform.vars.sold_estimates_view}`

### 3. vw_master_tracker_export
- **Descripción**: Exportación principal del tracker con información completa de trabajos
- **Dataset**: Silver
- **Variables**: `${dataform.vars.master_tracker_export_view}`
- **Función**: Utiliza `convert_utc_localtz` para conversión de zona horaria

## Variables de Configuración

### Dataset Raw Dinámico
El `raw_dataset` se genera automáticamente durante el despliegue basado en el `company_project_id` de la tabla companies:

- **Formato**: `servicetitan_<project_id>`
- **Transformación**: Los guiones (`-`) se reemplazan por guiones bajos (`_`) SOLO en el nombre del dataset
- **Regla**: Los datasets de BigQuery no pueden contener guiones, pero los proyectos sí
- **Ejemplos**:
  - Project ID: `shape-mhs-1` → Dataset: `servicetitan_shape_mhs_1`
  - Project ID: `company-abc-123` → Dataset: `servicetitan_company_abc_123`
  - Project ID: `test-project` → Dataset: `servicetitan_test_project`

### dataform.json
```json
{
  "vars": {
    "raw_dataset": "servicetitan_<project_id>",
    "bronze_dataset": "bronze",
    "new_customer_list_view": "vw_new_customer_list",
    "sold_estimates_view": "vw_sold_estimates",
    "master_tracker_export_view": "vw_master_tracker_export"
  }
}
```

**Nota**: El `raw_dataset` se genera dinámicamente durante el despliegue basado en el `company_project_id` de la tabla companies. Los guiones se reemplazan por guiones bajos SOLO en el nombre del dataset para cumplir con las reglas de BigQuery. Por ejemplo:
- `company_project_id`: "shape-mhs-1" → `raw_dataset`: "servicetitan_shape_mhs_1"
- `company_project_id`: "company-abc-123" → `raw_dataset`: "servicetitan_company_abc_123"

## Gestión de Estados

### company_ltm_status
- **-1**: No replicar
- **0**: Nunca ha replicado y lo requiere
- **1**: Replicado con éxito
- **2**: Replicado con error

## Comandos Principales

### Desarrollo
```bash
# Compilar Dataform
make compile

# Desplegar a desarrollo
make deploy-dev

# Verificar calidad
make quality-check
```

### QA
```bash
# Desplegar a QA
make deploy-qa

# Ver estado
make status
```

### Producción
```bash
# Ver estado actual
make status

# Desplegar solo pendientes
make deploy-prod

# Reintentar errores
make deploy-retry
```

### Gestión de Tabla Companies
```bash
# Agregar columnas LTM
make add-ltm-columns

# Ver información
make companies-info

# Actualizar estados de ejemplo (testing)
make update-sample-statuses
```

### Funciones Personalizadas
```bash
# Crear función convert_utc_localtz
make create-functions
```

### Pruebas
```bash
# Probar generación dinámica de raw_dataset
make test-raw-dataset
```

## Flujo de Trabajo por Ambiente

### Desarrollo
1. Compilar con `make compile`
2. Probar en dev con `make deploy-dev`
3. Verificar calidad con `make quality-check`

### QA
1. Desplegar a QA con `make deploy-qa`
2. Verificar calidad con `make quality-check`
3. Verificar estado con `make status`

### Producción
1. Verificar estado con `make status`
2. Desplegar pendientes con `make deploy-prod`
3. Verificar calidad con `make quality-check`
4. Reintentar errores si es necesario con `make deploy-retry`

## Estructura de Archivos

```
ltm_migration/
├── dataform/
│   ├── dataform.json
│   ├── includes/
│   │   └── config/
│   │       └── projects.py
│   └── definitions/
│       └── staging/
│           └── silver/
│               ├── vw_new_customer_list.sql
│               ├── vw_sold_estimates.sql
│               └── vw_master_tracker_export.sql
├── scripts/
│   ├── setup/
│   │   ├── initial_setup.py
│   │   ├── create_companies_table.py
│   │   ├── create_functions.py
│   │   └── create_convert_utc_localtz_function.sql
│   ├── deployment/
│   │   └── deploy_to_projects.py
│   └── monitoring/
│       └── quality_checks.py
├── docs/
│   └── README.md
├── tests/
├── requirements.txt
├── Makefile
└── .env.example
```

## Funciones Personalizadas

### convert_utc_localtz
- **Propósito**: Convertir fechas UTC a zona horaria local
- **Parámetros**: 
  - `utc_timestamp`: TIMESTAMP en UTC
  - `timezone_name`: STRING con nombre de zona horaria
- **Retorna**: DATETIME en zona horaria local
- **Ejemplo**: `convert_utc_localtz('2024-01-01 12:00:00 UTC', 'MONARCH')`

## Notas Importantes

1. **Variables**: Todas las referencias a proyectos y datasets usan variables de Dataform
2. **Funciones**: La función `convert_utc_localtz` debe crearse en cada proyecto antes del despliegue
3. **Estados**: El sistema usa `company_ltm_status` para gestionar replicaciones
4. **Ambientes**: Desarrollo y QA son para pruebas, producción es el ambiente final

## Próximos Pasos

1. ✅ Configurar estructura del proyecto
2. ✅ Implementar vistas parametrizadas
3. ✅ Crear scripts de gestión de estados
4. ✅ Implementar funciones personalizadas
5. ⏳ Configurar despliegue automático
6. ⏳ Implementar monitoreo continuo
7. ⏳ Agregar más vistas según necesidades 