-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Calidad · Silver
-- MAGIC
-- MAGIC Corre después del `MERGE` a silver, antes de tocar gold. Mismo patrón que
-- MAGIC en bronze: cada control se registra en `ops.log_calidad` y un
-- MAGIC `assert_true()` final corta la tarea si alguno `FAIL` no pasó.

-- COMMAND ----------

CREATE WIDGET TEXT run_id DEFAULT "manual";

-- COMMAND ----------

-- MAGIC %md ### `row_hash` único

-- COMMAND ----------

INSERT INTO kiosco_la_esquina.ops.log_calidad
SELECT
  :run_id, 'silver', 'row_hash_unico', 'row_hash no se repite en silver', 'FAIL',
  COUNT(*), COUNT(*) = 0, current_timestamp()
FROM (
  SELECT row_hash FROM kiosco_la_esquina.silver.ventas
  GROUP BY row_hash HAVING COUNT(*) > 1
);

-- COMMAND ----------

-- MAGIC %md ### Ninguna venta sin producto, sucursal o fecha

-- COMMAND ----------

INSERT INTO kiosco_la_esquina.ops.log_calidad
SELECT
  :run_id, 'silver', 'fks_no_nulas', 'producto_id/sucursal_nombre/fecha_hora nunca nulos', 'FAIL',
  COUNT_IF(producto_id IS NULL OR sucursal_nombre IS NULL OR fecha_hora IS NULL),
  COUNT_IF(producto_id IS NULL OR sucursal_nombre IS NULL OR fecha_hora IS NULL) = 0,
  current_timestamp()
FROM kiosco_la_esquina.silver.ventas;

-- COMMAND ----------

-- MAGIC %md ### Fechas dentro del rango válido del negocio (desde la apertura hasta hoy)

-- COMMAND ----------

INSERT INTO kiosco_la_esquina.ops.log_calidad
SELECT
  :run_id, 'silver', 'fechas_validas', 'fecha_hora entre 2026-09-01 y hoy', 'FAIL',
  COUNT_IF(fecha_hora < TIMESTAMP'2026-09-01' OR fecha_hora > current_timestamp()),
  COUNT_IF(fecha_hora < TIMESTAMP'2026-09-01' OR fecha_hora > current_timestamp()) = 0,
  current_timestamp()
FROM kiosco_la_esquina.silver.ventas;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### `precio_unitario` nulo tras el cast
-- MAGIC Un NULL acá indica un formato que la limpieza no contempló. Menos del 1%
-- MAGIC queda como `WARN`; más, como `FAIL`.

-- COMMAND ----------

INSERT INTO kiosco_la_esquina.ops.log_calidad
SELECT
  :run_id, 'silver', 'precio_unitario_castea_bien',
  '% de filas donde el cast de precio_unitario dio NULL',
  CASE WHEN pct < 1 THEN 'WARN' ELSE 'FAIL' END,
  nulos,
  pct < 1,
  current_timestamp()
FROM (
  SELECT
    COUNT_IF(precio_unitario IS NULL) AS nulos,
    COALESCE(100.0 * COUNT_IF(precio_unitario IS NULL) / NULLIF(COUNT(*), 0), 0) AS pct
  FROM kiosco_la_esquina.silver.ventas
);

-- COMMAND ----------

SELECT chequeo, severidad, filas_afectadas, ok
FROM kiosco_la_esquina.ops.log_calidad
WHERE run_id = :run_id AND capa = 'silver'
ORDER BY ejecutado_en DESC;

-- COMMAND ----------

SELECT assert_true(
  (SELECT COUNT(*) FROM kiosco_la_esquina.ops.log_calidad
   WHERE run_id = :run_id AND capa = 'silver' AND severidad = 'FAIL' AND NOT ok) = 0,
  'Calidad de silver FALLÓ. Revisar ops.log_calidad antes de seguir a gold.'
) AS gate_silver;
