-- Script para crear la función convert_utc_localtz en BigQuery
-- Esta función convierte fechas UTC a zona horaria local

CREATE OR REPLACE FUNCTION `${dataform.projectId}.${dataform.vars.bronze_dataset}.convert_utc_localtz`(
 SELECT  
      CASE 
        WHEN company_timezone = 'PST' THEN CAST(DATETIME(utc_time, 'America/Los_Angeles') AS TIMESTAMP)
        WHEN company_timezone = 'EST' THEN CAST(DATETIME(utc_time, 'America/New_York') AS TIMESTAMP)
        WHEN company_timezone = 'CST' THEN CAST(DATETIME(utc_time, 'America/Chicago') AS TIMESTAMP)
        ELSE utc_time  -- UTC por defecto
      END
    FROM platform-partners-qua.settings.companies
    WHERE company_name = companyname
);  

-- Ejemplo de uso:
-- SELECT convert_utc_localtz('2024-01-01 12:00:00 UTC', 'America/New_York') as local_time;
-- SELECT convert_utc_localtz('2024-01-01 12:00:00 UTC', 'MONARCH') as local_time; 