-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Calidad · Gold
-- MAGIC
-- MAGIC Última red de seguridad del pipeline, después de cargar la fact.

-- COMMAND ----------

CREATE WIDGET TEXT run_id DEFAULT "manual";

-- COMMAND ----------

-- MAGIC %md ### Integridad referencial: 0 FKs huérfanas en la fact

-- COMMAND ----------

INSERT INTO kiosco_la_esquina.ops.log_calidad
SELECT
  :run_id, 'gold', 'integridad_referencial',
  '0 filas de fact_ventas sin match en alguna dimensión', 'FAIL',
  COUNT(*), COUNT(*) = 0, current_timestamp()
FROM kiosco_la_esquina.gold.fact_ventas f
LEFT JOIN kiosco_la_esquina.gold.dim_tiempo      t  ON f.tiempo_sk      = t.tiempo_sk
LEFT JOIN kiosco_la_esquina.gold.dim_sucursal    s  ON f.sucursal_sk    = s.sucursal_sk
LEFT JOIN kiosco_la_esquina.gold.dim_producto    p  ON f.producto_sk    = p.producto_sk
LEFT JOIN kiosco_la_esquina.gold.dim_empleado    e  ON f.empleado_sk    = e.empleado_sk
LEFT JOIN kiosco_la_esquina.gold.dim_transaccion tr ON f.transaccion_sk = tr.transaccion_sk
WHERE t.tiempo_sk IS NULL OR s.sucursal_sk IS NULL OR p.producto_sk IS NULL
   OR e.empleado_sk IS NULL OR tr.transaccion_sk IS NULL;

-- COMMAND ----------

-- MAGIC %md ### Máximo 1 versión vigente por business key, en las dos SCD Tipo 2

-- COMMAND ----------

INSERT INTO kiosco_la_esquina.ops.log_calidad
SELECT
  :run_id, 'gold', 'un_solo_vigente_producto', 'Máximo 1 is_current=true por producto_id', 'FAIL',
  COUNT(*), COUNT(*) = 0, current_timestamp()
FROM (
  SELECT producto_id FROM kiosco_la_esquina.gold.dim_producto
  WHERE is_current = true
  GROUP BY producto_id HAVING COUNT(*) > 1
);

INSERT INTO kiosco_la_esquina.ops.log_calidad
SELECT
  :run_id, 'gold', 'un_solo_vigente_empleado', 'Máximo 1 is_current=true por vendedor_id', 'FAIL',
  COUNT(*), COUNT(*) = 0, current_timestamp()
FROM (
  SELECT vendedor_id FROM kiosco_la_esquina.gold.dim_empleado
  WHERE is_current = true AND vendedor_id IS NOT NULL
  GROUP BY vendedor_id HAVING COUNT(*) > 1
);

-- COMMAND ----------

-- MAGIC %md ### Sin `row_hash` duplicados en la fact

-- COMMAND ----------

INSERT INTO kiosco_la_esquina.ops.log_calidad
SELECT
  :run_id, 'gold', 'row_hash_unico_fact', 'row_hash no se repite en fact_ventas', 'FAIL',
  COUNT(*), COUNT(*) = 0, current_timestamp()
FROM (
  SELECT row_hash FROM kiosco_la_esquina.gold.fact_ventas
  GROUP BY row_hash HAVING COUNT(*) > 1
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Reconciliación: silver − fact tiene que ser exactamente lo excluido
-- MAGIC Si la diferencia no cierra, se perdieron o se duplicaron ventas entre capas.

-- COMMAND ----------

INSERT INTO kiosco_la_esquina.ops.log_calidad
SELECT
  :run_id, 'gold', 'reconciliacion_fact_silver',
  concat('silver(', total_silver, ') - fact(', total_fact, ') debe ser igual a excluidas(', excluidas, ')'),
  'FAIL',
  ABS((total_silver - total_fact) - excluidas),
  (total_silver - total_fact) = excluidas,
  current_timestamp()
FROM (
  SELECT
    (SELECT COUNT(*) FROM kiosco_la_esquina.silver.ventas) AS total_silver,
    (SELECT COUNT(*) FROM kiosco_la_esquina.gold.fact_ventas) AS total_fact,
    (SELECT COUNT(*) FROM kiosco_la_esquina.silver.ventas
     WHERE cantidad <= 0 AND estado_venta = 'aprobada') AS excluidas
);

-- COMMAND ----------

SELECT chequeo, severidad, filas_afectadas, ok, descripcion
FROM kiosco_la_esquina.ops.log_calidad
WHERE run_id = :run_id AND capa = 'gold'
ORDER BY ejecutado_en DESC;

-- COMMAND ----------

SELECT assert_true(
  (SELECT COUNT(*) FROM kiosco_la_esquina.ops.log_calidad
   WHERE run_id = :run_id AND capa = 'gold' AND severidad = 'FAIL' AND NOT ok) = 0,
  'Calidad de gold FALLÓ. Revisar ops.log_calidad.'
) AS gate_gold;
