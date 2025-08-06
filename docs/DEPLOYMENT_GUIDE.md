# Guía de Despliegue - LTM Migration

## 🚀 Configuración Inicial

### 1. Preparar el Repositorio

```bash
# Clonar el repositorio (cuando esté en GitHub)
git clone https://github.com/tu-usuario/ltm-migration.git
cd ltm-migration

# Instalar dependencias
make install

# Configurar el repositorio
make setup-repo
```

### 2. Verificar Configuración

```bash
# Probar conexión con BigQuery
make test-raw-dataset

# Ver estado de compañías
make status
```

## 📋 Configuración de Dataform

### 1. Crear Proyecto Dataform

1. Ve a [Dataform](https://dataform.co/)
2. Crea un nuevo proyecto
3. Conecta tu repositorio de GitHub
4. Configura las credenciales de BigQuery

### 2. Configurar Variables de Entorno

En Dataform, configura las siguientes variables:
- `GOOGLE_APPLICATION_CREDENTIALS`: Ruta a tu archivo de credenciales
- `BIGQUERY_PROJECT_ID`: ID del proyecto de BigQuery

## 🔧 Despliegue por Ambientes

### Desarrollo (Pruebas Unitarias)

```bash
# Compilar Dataform
make compile

# Desplegar a desarrollo
make deploy-dev

# Verificar calidad
make quality-check
```

### QA (Pruebas Funcionales)

```bash
# Desplegar a QA
make deploy-qa

# Verificar calidad
make quality-check

# Ver estado
make status
```

### Producción (Despliegue Real)

```bash
# Ver estado actual
make status

# Crear funciones personalizadas (solo la primera vez)
make create-functions

# Desplegar solo compañías pendientes
make deploy-prod

# Verificar calidad
make quality-check

# Reintentar errores si es necesario
make deploy-retry
```

## 🧪 Pruebas

### Pruebas de Configuración

```bash
# Probar generación de raw_dataset
make test-raw-dataset

# Ver información de tabla companies
make companies-info
```

### Pruebas de Despliegue

```bash
# Probar en desarrollo
make deploy-dev

# Verificar vistas creadas
make quality-check
```

## 📊 Monitoreo

### Ver Estado de Replicaciones

```bash
# Ver resumen de estados
make status

# Ver compañías pendientes
python scripts/deployment/deploy_to_projects.py --status
```

### Verificar Calidad de Datos

```bash
# Verificar calidad en QA
make quality-check

# Verificar calidad en producción específica
python scripts/monitoring/quality_checks.py --project <project-id> --dataset silver --all-tables
```

## 🔄 Flujo de Trabajo Típico

### Para Nuevas Vistas

1. **Desarrollo**:
   ```bash
   # Crear nueva vista en dataform/definitions/staging/silver/
   # Compilar y probar
   make compile
   make deploy-dev
   ```

2. **QA**:
   ```bash
   # Desplegar a QA
   make deploy-qa
   make quality-check
   ```

3. **Producción**:
   ```bash
   # Actualizar estado de compañías a 0 (pendiente)
   # Desplegar
   make deploy-prod
   ```

### Para Nuevas Compañías

1. **Agregar a la tabla companies**:
   ```sql
   INSERT INTO `platform-partners-qua.settings.companies` 
   (company_id, company_name, project_id, company_ltm_status)
   VALUES ('nueva-company', 'Nueva Compañía', 'nuevo-project-id', 0);
   ```

2. **Desplegar**:
   ```bash
   make deploy-prod
   ```

## 🚨 Solución de Problemas

### Error de Conexión

```bash
# Verificar credenciales
gcloud auth application-default login

# Verificar proyecto
gcloud config set project platform-partners-qua
```

### Error de Funciones

```bash
# Recrear funciones
make create-functions

# Verificar que se crearon
bq show --dataset <project-id>:bronze
```

### Error de Despliegue

```bash
# Ver logs
cd dataform && dataform compile --verbose

# Reintentar
make deploy-retry
```

## 📈 Métricas de Éxito

- ✅ Todas las compañías con `company_ltm_status = 1`
- ✅ Vistas creadas en todos los proyectos
- ✅ Calidad de datos verificada
- ✅ Funciones personalizadas creadas

## 🔐 Seguridad

- Las credenciales de BigQuery deben estar seguras
- No subir archivos de credenciales al repositorio
- Usar variables de entorno para configuraciones sensibles
- Revisar permisos de Dataform regularmente

## 📞 Soporte

Para problemas técnicos:
1. Revisar logs de Dataform
2. Verificar estado de compañías: `make status`
3. Probar conexión: `make test-raw-dataset`
4. Revisar documentación en `docs/README.md` 