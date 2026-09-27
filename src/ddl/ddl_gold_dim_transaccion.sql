-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DDL · gold.dim_transaccion (junk dimension)
-- MAGIC
-- MAGIC Combina `metodo_pago` + `estado_venta` en una sola dimensión chica, en vez
-- MAGIC de separarlas o de tratar `estado_venta` como un mecanismo de eventos
-- MAGIC aparte. El grano de la fact ya es "un evento" (una línea con su propio
-- MAGIC ticket y fecha_hora) — una devolución ya llega como fila nueva, así que
-- MAGIC `estado_venta` es simplemente un atributo descriptivo más, igual que el
-- MAGIC método de pago. Ambos son de cardinalidad baja y sin entidad maestra
-- MAGIC propia, el patrón clásico de Kimball para esto es una junk dimension.
-- MAGIC
-- MAGIC Un `metodo_pago` nulo en la fuente se resuelve como la combinación
-- MAGIC `'no_informado'`, sin necesitar una fila "desconocido" aparte.
-- MAGIC
-- MAGIC Solo crece: nunca se actualiza ni se borra una combinación existente.

-- COMMAND ----------

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
