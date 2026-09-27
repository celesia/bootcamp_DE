-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DDL · gold.dim_producto (SCD Tipo 2 sobre `precio_lista`)
-- MAGIC
-- MAGIC Business key: `producto_id` (lo da la API). Se abre una versión nueva cada
-- MAGIC vez que cambia `precio_lista` — el catálogo sube de precio cada 12 a 25
-- MAGIC días según el producto, así que desde la primera carga ya hay más de una
-- MAGIC versión para varios productos.
-- MAGIC
-- MAGIC `precio_lista` es un atributo de la dimensión (cambia lento, cada tantos
-- MAGIC días) — distinto de `precio_unitario` en la fact, que es una métrica
-- MAGIC transaccional (cambia en cada venta según el descuento aplicado).

-- COMMAND ----------

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
