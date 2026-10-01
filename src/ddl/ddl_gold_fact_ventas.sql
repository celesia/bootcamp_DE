-- Databricks notebook source
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
