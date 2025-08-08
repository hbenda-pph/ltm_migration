config {
  type: "function",
  schema: "bronze",
  name: "convert_utc_localtz",
  description: "Función para convertir fechas UTC a zona horaria local"
}

CREATE OR REPLACE FUNCTION `${dataform.projectId}.bronze.convert_utc_localtz`(
  utc_timestamp TIMESTAMP,
  timezone_name STRING
) AS (
  DATETIME(utc_timestamp, timezone_name)
); 