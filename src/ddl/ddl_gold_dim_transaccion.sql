-- Databricks notebook source
CREATE TABLE IF NOT EXISTS kiosco_la_esquina.gold.dim_transaccion (
  transaccion_sk STRING  COMMENT 'MD5(metodo_pago + estado_venta)',
  metodo_pago    STRING  COMMENT "'no_informado' si vino nulo en la fuente",
  estado_venta   STRING  COMMENT 'aprobada | anulada | devolucion',
  _created_at    TIMESTAMP
)
USING DELTA
COMMENT 'Junk dimension: combinaciones de método de pago + estado de venta. Solo crece';

-- COMMAND ----------

DESCRIBE TABLE kiosco_la_esquina.gold.dim_transaccion;
