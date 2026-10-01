-- Databricks notebook source
CREATE OR REPLACE TEMPORARY VIEW dim_producto_versiones AS
WITH precios_por_dia AS (
  SELECT DISTINCT producto_id, producto_nombre, categoria, precio_lista, DATE(fecha_hora) AS fecha
  FROM kiosco_la_esquina.silver.ventas
),
marcado_cambio AS (
  SELECT *,
    LAG(precio_lista) OVER (PARTITION BY producto_id ORDER BY fecha) AS precio_anterior
  FROM precios_por_dia
),
inicios_de_version AS (
  SELECT producto_id, producto_nombre, categoria, precio_lista, fecha AS valid_from
  FROM marcado_cambio
  WHERE precio_anterior IS NULL OR precio_anterior != precio_lista
),
con_fin AS (
  SELECT *,
    LEAD(valid_from) OVER (PARTITION BY producto_id ORDER BY valid_from) AS siguiente_inicio
  FROM inicios_de_version
)
SELECT
  md5(concat_ws('|', CAST(producto_id AS STRING), CAST(CAST(valid_from AS DATE) AS STRING))) AS producto_sk,
  producto_id,
  producto_nombre,
  categoria,
  precio_lista,
  CAST(valid_from AS TIMESTAMP) AS valid_from,
  CASE WHEN siguiente_inicio IS NULL THEN TIMESTAMP'9999-12-31'
       ELSE CAST(siguiente_inicio AS TIMESTAMP) - INTERVAL 1 SECOND
  END AS valid_to,
  siguiente_inicio IS NULL AS is_current
FROM con_fin;

-- COMMAND ----------

SELECT producto_id, producto_nombre, COUNT(*) AS versiones
FROM dim_producto_versiones
GROUP BY producto_id, producto_nombre
ORDER BY producto_id;

-- COMMAND ----------

MERGE INTO kiosco_la_esquina.gold.dim_producto AS destino
USING dim_producto_versiones AS origen
ON  destino.producto_id = origen.producto_id
AND destino.valid_from  = origen.valid_from
AND destino.is_current  = true
WHEN MATCHED AND origen.is_current = false THEN UPDATE SET
  destino.valid_to   = origen.valid_to,
  destino.is_current = false;

-- COMMAND ----------

INSERT INTO kiosco_la_esquina.gold.dim_producto
  (producto_sk, producto_id, producto_nombre, categoria, precio_lista, valid_from, valid_to, is_current)
SELECT v.producto_sk, v.producto_id, v.producto_nombre, v.categoria, v.precio_lista,
       v.valid_from, v.valid_to, v.is_current
FROM dim_producto_versiones v
LEFT ANTI JOIN kiosco_la_esquina.gold.dim_producto d
  ON v.producto_sk = d.producto_sk;

-- COMMAND ----------

SELECT producto_id, COUNT(*) AS versiones_vigentes
FROM kiosco_la_esquina.gold.dim_producto
WHERE is_current = true
GROUP BY producto_id
HAVING COUNT(*) > 1;
