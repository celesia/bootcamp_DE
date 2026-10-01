-- Databricks notebook source
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
