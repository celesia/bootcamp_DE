-- Databricks notebook source
MERGE INTO kiosco_la_esquina.gold.dim_empleado AS destino
USING (SELECT '-1' AS empleado_sk) AS origen
ON destino.empleado_sk = origen.empleado_sk
WHEN NOT MATCHED THEN INSERT
  (empleado_sk, vendedor_id, vendedor_nombre, sucursal_nombre, valid_from, valid_to, is_current)
  VALUES ('-1', NULL, 'Desconocido', NULL, TIMESTAMP'1900-01-01', TIMESTAMP'9999-12-31', true);

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW dim_empleado_versiones AS
WITH sucursal_por_dia AS (
  SELECT DISTINCT vendedor_id, vendedor_nombre, sucursal_nombre, DATE(fecha_hora) AS fecha
  FROM kiosco_la_esquina.silver.ventas
  WHERE vendedor_id IS NOT NULL
),
marcado_cambio AS (
  SELECT *,
    LAG(sucursal_nombre) OVER (PARTITION BY vendedor_id ORDER BY fecha) AS sucursal_anterior
  FROM sucursal_por_dia
),
inicios_de_version AS (
  SELECT vendedor_id, vendedor_nombre, sucursal_nombre, fecha AS valid_from
  FROM marcado_cambio
  WHERE sucursal_anterior IS NULL OR sucursal_anterior != sucursal_nombre
),
con_fin AS (
  SELECT *,
    LEAD(valid_from) OVER (PARTITION BY vendedor_id ORDER BY valid_from) AS siguiente_inicio
  FROM inicios_de_version
)
SELECT
  md5(concat_ws('|', CAST(vendedor_id AS STRING), CAST(CAST(valid_from AS DATE) AS STRING))) AS empleado_sk,
  vendedor_id,
  vendedor_nombre,
  sucursal_nombre,
  CAST(valid_from AS TIMESTAMP) AS valid_from,
  CASE WHEN siguiente_inicio IS NULL THEN TIMESTAMP'9999-12-31'
       ELSE CAST(siguiente_inicio AS TIMESTAMP) - INTERVAL 1 SECOND
  END AS valid_to,
  siguiente_inicio IS NULL AS is_current
FROM con_fin;

-- COMMAND ----------

SELECT vendedor_id, vendedor_nombre, COUNT(*) AS versiones
FROM dim_empleado_versiones
GROUP BY vendedor_id, vendedor_nombre
ORDER BY vendedor_id;

-- COMMAND ----------

MERGE INTO kiosco_la_esquina.gold.dim_empleado AS destino
USING dim_empleado_versiones AS origen
ON  destino.vendedor_id = origen.vendedor_id
AND destino.valid_from  = origen.valid_from
AND destino.is_current  = true
WHEN MATCHED AND origen.is_current = false THEN UPDATE SET
  destino.valid_to   = origen.valid_to,
  destino.is_current = false;

-- COMMAND ----------

INSERT INTO kiosco_la_esquina.gold.dim_empleado
  (empleado_sk, vendedor_id, vendedor_nombre, sucursal_nombre, valid_from, valid_to, is_current)
SELECT v.empleado_sk, v.vendedor_id, v.vendedor_nombre, v.sucursal_nombre,
       v.valid_from, v.valid_to, v.is_current
FROM dim_empleado_versiones v
LEFT ANTI JOIN kiosco_la_esquina.gold.dim_empleado d
  ON v.empleado_sk = d.empleado_sk;

-- COMMAND ----------

SELECT vendedor_id, COUNT(*) AS versiones_vigentes
FROM kiosco_la_esquina.gold.dim_empleado
WHERE is_current = true AND vendedor_id IS NOT NULL
GROUP BY vendedor_id
HAVING COUNT(*) > 1;
