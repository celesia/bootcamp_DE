-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DDL · gold.fact_ventas
-- MAGIC
-- MAGIC Grano: una fila por línea de venta válida (una por producto por ticket).
-- MAGIC `row_hash` se hereda directamente de silver como clave primaria de la
-- MAGIC fact — el grano no cambia entre silver y gold, solo se excluyen las
-- MAGIC filas de `cantidad = 0` con `estado_venta = 'aprobada'` (dato sucio, no
-- MAGIC una venta real), y esa exclusión queda registrada en `ops.log_calidad`
-- MAGIC para que sea auditable.
-- MAGIC
-- MAGIC `ticket_id` es una dimensión degenerada: no amerita tabla propia porque
-- MAGIC es prácticamente único por fila, así que se queda directo en la fact.
-- MAGIC
-- MAGIC Se carga con `MERGE` sobre `row_hash` — idempotente, no duplica ni pierde
-- MAGIC histórico si el pipeline se reprocesa.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS kiosco_la_esquina.gold.fact_ventas (
  row_hash        STRING          COMMENT 'PK, heredado de silver',
  ticket_id       STRING          COMMENT 'Dimensión degenerada',
  tiempo_sk       INT             COMMENT 'FK -> dim_tiempo',
  sucursal_sk     STRING          COMMENT 'FK -> dim_sucursal',
  producto_sk     STRING          COMMENT 'FK -> dim_producto, resuelto por vigencia en fecha_hora',
  empleado_sk     STRING          COMMENT 'FK -> dim_empleado, resuelto por vigencia en fecha_hora (o -1 si desconocido)',
  transaccion_sk  STRING          COMMENT 'FK -> dim_transaccion',
  cantidad        INT,
  precio_unitario DECIMAL(10, 2),
  descuento       DECIMAL(5, 4),
  venta_neta      DECIMAL(12, 2) COMMENT 'cantidad * precio_unitario',
  _gold_loaded_at TIMESTAMP
)
USING DELTA
COMMENT 'Tabla de hechos: una fila por línea de venta válida';

-- COMMAND ----------

DESCRIBE TABLE kiosco_la_esquina.gold.fact_ventas;
