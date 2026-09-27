-- Databricks notebook source
-- MAGIC %md
-- MAGIC # ETL · gold.dim_transaccion (junk dimension)
-- MAGIC
-- MAGIC Solo crece: `MERGE ... WHEN NOT MATCHED THEN INSERT`, nunca actualiza ni
-- MAGIC borra una combinación existente. Un `metodo_pago` nulo en la fuente se
-- MAGIC resuelve acá como la combinación `'no_informado'`.

-- COMMAND ----------

MERGE INTO kiosco_la_esquina.gold.dim_transaccion AS destino
USING (
  SELECT DISTINCT
    md5(concat_ws('|', COALESCE(metodo_pago, 'no_informado'), estado_venta)) AS transaccion_sk,
    COALESCE(metodo_pago, 'no_informado') AS metodo_pago,
    estado_venta
  FROM kiosco_la_esquina.silver.ventas
) AS origen
ON destino.transaccion_sk = origen.transaccion_sk
WHEN NOT MATCHED THEN INSERT (transaccion_sk, metodo_pago, estado_venta, _created_at)
  VALUES (origen.transaccion_sk, origen.metodo_pago, origen.estado_venta, current_timestamp());

-- COMMAND ----------

-- Hasta 15 combinaciones posibles: 5 métodos (4 + no_informado) × 3 estados.
SELECT * FROM kiosco_la_esquina.gold.dim_transaccion ORDER BY metodo_pago, estado_venta;
