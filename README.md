# LTM Migration - Dataform Project

## Descripción del Proyecto

Este proyecto utiliza Dataform para gestionar la migración de vistas SQL a múltiples proyectos de BigQuery siguiendo una arquitectura de medalla (Bronze, Silver, Gold). **Gestiona estados de replicación desde la tabla `companies` existente en `platform-partners-qua.settings.companies`**.

## Arquitectura de Ambientes

### Gestión por Ambiente
- **DEV (`platform-partners-dev`)**: Pruebas unitarias y desarrollo
- **QA (`platform-partners-qua`)**: Pruebas funcionales y configuración
- **PROD**: Múltiples proyectos de producción por compañía

### Flujo de Trabajo
1. **Desarrollo**: Pruebas en DEV
2. **QA**: Validación funcional en QA
3. **Producción**: Despliegue inteligente basado en estados

## Gestión de Estados de Replicación

El sistema gestiona los estados de replicación mediante la tabla `companies`:

| Estado | Código | Descripción |
|--------|--------|-------------|
| NO_REPLICATE | -1 | No replicar |
| PENDING | 0 | Nunca ha replicado y lo requiere |
| SUCCESS | 1 | Replicado con éxito |
| ERROR | 2 | Replicado con error |
| IN_PROGRESS | 3 | Replicación en progreso |
| ROLLBACK | 4 | Rollback requerido |
| DEPRECATED | 5 | Compañía deprecada |

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

### 3. Agregar Columnas LTM a la Tabla
```bash
# Agregar columnas LTM a la tabla companies existente
make add-ltm-columns

# Ver información de la tabla
make companies-info
```

### 4. Configurar Credenciales
```bash
# Autenticación con Google Cloud
gcloud auth application-default login

# O usar service account
export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account.json"
```

## Uso por Ambiente

### Desarrollo (DEV)
```bash
# Compilar y probar
make compile
make deploy-dev

# Verificar calidad
make quality-check
```

### QA
```bash
# Desplegar a QA para pruebas funcionales
make deploy-qa

# Verificar calidad
make quality-check

# Ver estado de replicaciones
make status
```

### Producción
```bash
# Ver estado actual
make status

# Desplegar solo compañías pendientes
make deploy-prod

# Reintentar compañías con errores
make deploy-retry
```

## Comandos Principales

### Gestión de Estados
```bash
# Ver estado de replicaciones
make status

# Desplegar solo pendientes
make deploy-prod

# Reintentar errores
make deploy-retry
```

### Despliegue por Ambiente
```bash
# Desarrollo (pruebas unitarias)
make deploy-dev

# QA (pruebas funcionales)
make deploy-qa

# Producción (solo pendientes)
make deploy-prod
```

### Configuración de Tabla
```bash
# Agregar columnas LTM
make add-ltm-columns

# Ver información de tabla
make companies-info

# Actualizar estados de ejemplo (solo testing)
make update-sample-statuses
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

## Tabla Companies

La tabla `platform-partners-qua.settings.companies` gestiona toda la configuración:

### Columnas LTM Agregadas
```sql
-- Columnas que se agregan a la tabla existente
ALTER TABLE `platform-partners-qua.settings.companies` 
ADD COLUMN company_ltm_status INT64,
ADD COLUMN last_replication_error STRING,
ADD COLUMN last_replication_date TIMESTAMP
```

### Estados de Replicación
- **-1 (NO_REPLICATE)**: Compañía excluida de replicación
- **0 (PENDING)**: Pendiente de replicación inicial
- **1 (SUCCESS)**: Replicación exitosa
- **2 (ERROR)**: Error en replicación, requiere reintento
- **3 (IN_PROGRESS)**: Replicación en curso
- **4 (ROLLBACK)**: Requiere rollback
- **5 (DEPRECATED)**: Compañía deprecada

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

## Flujo de Trabajo por Ambiente

### 1. Desarrollo (DEV)
```bash
# Configuración inicial
make add-ltm-columns

# Desarrollo y pruebas
make compile
make deploy-dev
make quality-check
```

### 2. QA
```bash
# Pruebas funcionales
make deploy-qa
make quality-check
make status
```

### 3. Producción
```bash
# Despliegue inteligente
make status
make deploy-prod
make quality-check
make deploy-retry  # Si hay errores
```

## Comandos Disponibles

### Gestión de Estados
- `make status` - Ver estado de replicaciones
- `make deploy-prod` - Desplegar solo pendientes
- `make deploy-retry` - Reintentar errores

### Despliegue por Ambiente
- `make deploy-dev` - Solo desarrollo (pruebas unitarias)
- `make deploy-qa` - Solo QA (pruebas funcionales)
- `make deploy-prod` - Solo compañías pendientes en producción

### Configuración
- `make add-ltm-columns` - Agregar columnas LTM
- `make companies-info` - Ver información de tabla
- `make update-sample-statuses` - Actualizar estados de ejemplo

### Calidad
- `make quality-check` - Verificar calidad de datos

## Troubleshooting

### Errores Comunes
1. **Credenciales**: Verificar autenticación de Google Cloud
2. **Permisos**: Asegurar acceso a proyectos BigQuery
3. **Tabla Companies**: Verificar que existe `platform-partners-qua.settings.companies`
4. **Columnas LTM**: Verificar que existen las columnas LTM
5. **Estados**: Verificar valores válidos en `company_ltm_status`

### Logs
- Dataform logs: `dataform logs`
- BigQuery logs: Console de Google Cloud
- Deployment logs: Salida de scripts
- Estados: Consultar tabla `companies`

## Contribución

1. Crear branch para nueva funcionalidad
2. Implementar cambios en Dataform
3. Probar en ambiente de desarrollo (`make deploy-dev`)
4. Validar en QA (`make deploy-qa`)
5. Crear Pull Request
6. Revisar y aprobar cambios
7. Desplegar a producción (`make deploy-prod`)

## Contacto

Para soporte técnico o preguntas sobre el proyecto, contactar al equipo de datos. 