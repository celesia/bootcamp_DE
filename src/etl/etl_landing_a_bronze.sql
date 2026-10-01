-- Databricks notebook source
CREATE WIDGET TEXT run_id DEFAULT "manual";

-- COMMAND ----------

COPY INTO kiosco_la_esquina.bronze.ventas
FROM (
  SELECT
    inline(data),
    _metadata.file_path  AS _source_file,
    current_timestamp()  AS _ingest_timestamp,
    :run_id              AS _run_id
  FROM '/Volumes/kiosco_la_esquina/landing/raw_ventas/'
)
FILEFORMAT = JSON
FORMAT_OPTIONS ('multiLine' = 'true', 'primitivesAsString' = 'true');

-- COMMAND ----------

SELECT _source_file, COUNT(*) AS filas
FROM kiosco_la_esquina.bronze.ventas
WHERE _run_id = :run_id
GROUP BY _source_file;
