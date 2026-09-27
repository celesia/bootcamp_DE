-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DDL · gold.dim_sucursal (SCD Tipo 1)
-- MAGIC
-- MAGIC 5 sucursales fijas. Business key: `sucursal_nombre` ya normalizado (sin
-- MAGIC mayúsculas forzadas ni espacios de más — esa limpieza pasa en silver).
-- MAGIC Si una sucursal "cerrara", en este negocio abriría con un nombre/ID nuevo,
-- MAGIC así que no hace falta versionar histórico acá — de ahí SCD Tipo 1.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS kiosco_la_esquina.gold.dim_sucursal (
  sucursal_sk    STRING  COMMENT 'MD5 de sucursal_nombre normalizado',
  sucursal_nombre STRING  COMMENT 'Business key',
  ciudad          STRING,
  departamento    STRING,
  _updated_at     TIMESTAMP
)
USING DELTA
COMMENT 'SCD 1: se sobrescribe el valor viejo con el nuevo, sin conservar historial';

-- COMMAND ----------

DESCRIBE TABLE kiosco_la_esquina.gold.dim_sucursal;
