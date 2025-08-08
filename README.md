# LTM Migration - Dataform Project

## Descripción
Proyecto simple para replicar 3 vistas SQL en múltiples proyectos de BigQuery usando Dataform.

## Vistas Implementadas
1. **vw_new_customer_list** - Lista de nuevos clientes
2. **vw_sold_estimates** - Estimaciones vendidas  
3. **vw_master_tracker_export** - Exportación principal del tracker

## Funciones Implementadas
1. **fn_convert_utc_localtz** - Función para convertir fechas UTC a zona horaria local

## Estructura
```
dataform/
├── dataform.json
└── definitions/
    ├── views/
    │   ├── vw_new_customer_list.sql
    │   ├── vw_sold_estimates.sql
    │   └── vw_master_tracker_export.sql
    └── functions/
        └── fn_convert_utc_localtz.sql
```

## Uso

### Opción 1: Cloud Run Job (Recomendado para GCloud)
```bash
# Desplegar Cloud Run job
./cloud-run-deploy.sh

# Ejecutar deployment
gcloud run jobs execute ltm-deployment-job --region us-central1
```

### Opción 2: Despliegue Automático Local
```bash
# Desplegar a todos los proyectos automáticamente
python deploy_all.py
```

Este script:
1. Lee la tabla `companies` desde `platform-partners-pro.settings.companies`
2. Itera sobre todos los proyectos con `company_ltm_status = 0`
3. Actualiza `dataform.json` automáticamente
4. Ejecuta `dataform run` para cada proyecto

### Opción 3: Despliegue Manual
1. **Configurar Proyecto**
   Editar `dataform/dataform.json`:
   ```json
   {
     "warehouse": "bigquery",
     "defaultSchema": "silver",
     "defaultDatabase": "TU-PROJECT-ID",
     "vars": {
       "raw_dataset": "servicetitan_TU_PROJECT_ID"
     }
   }
   ```

2. **Ejecutar Dataform**
   ```bash
   cd dataform
   dataform compile
   dataform run
   ```

## Proceso de Replicación
1. Cambiar `defaultDatabase` en `dataform.json` por cada proyecto
2. Cambiar `raw_dataset` en `vars` (reemplazar guiones por guiones bajos)
3. Ejecutar `dataform run`
4. Repetir para todas las compañías

## Ejemplo
Para proyecto `shape-mhs-1`:
```json
{
  "defaultDatabase": "shape-mhs-1",
  "vars": {
    "raw_dataset": "servicetitan_shape_mhs_1"
  }
}
``` 