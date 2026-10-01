-- Databricks notebook source
CREATE TABLE IF NOT EXISTS kiosco_la_esquina.gold.dim_empleado (
  empleado_sk     STRING    COMMENT 'MD5(vendedor_id + valid_from), o -1 para el miembro desconocido',
  vendedor_id     INT       COMMENT 'Business key. NULL para el miembro desconocido',
  vendedor_nombre STRING,
  sucursal_nombre STRING    COMMENT 'Atributo propio, no FK a dim_sucursal — mantiene la carga en paralelo',
  valid_from      TIMESTAMP,
  valid_to        TIMESTAMP COMMENT '9999-12-31 si sigue vigente',
  is_current      BOOLEAN
)
USING DELTA
COMMENT 'SCD 2: historial completo de a qué sucursal estuvo asignado cada vendedor';

-- COMMAND ----------

DESCRIBE TABLE kiosco_la_esquina.gold.dim_empleado;
