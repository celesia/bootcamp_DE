-- Databricks notebook source
CREATE TABLE IF NOT EXISTS kiosco_la_esquina.gold.dim_producto (
  producto_sk     STRING    COMMENT 'MD5(producto_id + valid_from) — cambia en cada versión',
  producto_id     INT       COMMENT 'Business key, estable entre versiones',
  producto_nombre STRING,
  categoria       STRING,
  precio_lista    DECIMAL(10, 2) COMMENT 'Atributo versionado: el precio de catálogo vigente en ese período',
  valid_from      TIMESTAMP,
  valid_to        TIMESTAMP COMMENT '9999-12-31 si sigue vigente',
  is_current      BOOLEAN
)
USING DELTA
COMMENT 'SCD 2: historial completo de precio_lista por producto';

-- COMMAND ----------

DESCRIBE TABLE kiosco_la_esquina.gold.dim_producto;
