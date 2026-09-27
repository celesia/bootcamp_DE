-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DDL · gold.dim_empleado (SCD Tipo 2 sobre sucursal asignada)
-- MAGIC
-- MAGIC Business key: `vendedor_id`. Se abre una versión nueva cada vez que cambia
-- MAGIC la sucursal asignada — hay traslados reales de empleados en la primera
-- MAGIC quincena de apertura del negocio, para tener historial real desde la
-- MAGIC primera carga.
-- MAGIC
-- MAGIC `sucursal_nombre` queda como atributo propio de esta tabla (no un FK a
-- MAGIC `dim_sucursal`) a propósito: las 5 dimensiones de gold se cargan en
-- MAGIC paralelo porque no dependen entre sí — un FK entre dos dimensiones
-- MAGIC rompería esa independencia.
-- MAGIC
-- MAGIC Incluye una fila "desconocido" (`empleado_sk = '-1'`) para las ventas
-- MAGIC donde `vendedor_id` viene nulo desde la fuente.

-- COMMAND ----------

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
