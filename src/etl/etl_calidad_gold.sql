-- Databricks notebook source
CREATE OR REPLACE TEMPORARY VIEW controles_gold AS
SELECT
  (SELECT COUNT(*)
   FROM kiosco_la_esquina.gold.fact_ventas f
   LEFT JOIN kiosco_la_esquina.gold.dim_tiempo      t  ON f.tiempo_sk      = t.tiempo_sk
   LEFT JOIN kiosco_la_esquina.gold.dim_sucursal    s  ON f.sucursal_sk    = s.sucursal_sk
   LEFT JOIN kiosco_la_esquina.gold.dim_producto    p  ON f.producto_sk    = p.producto_sk
   LEFT JOIN kiosco_la_esquina.gold.dim_empleado    e  ON f.empleado_sk    = e.empleado_sk
   LEFT JOIN kiosco_la_esquina.gold.dim_transaccion tr ON f.transaccion_sk = tr.transaccion_sk
   WHERE t.tiempo_sk IS NULL OR s.sucursal_sk IS NULL OR p.producto_sk IS NULL
      OR e.empleado_sk IS NULL OR tr.transaccion_sk IS NULL)                 AS fks_huerfanas,

  (SELECT COUNT(*) FROM (
     SELECT producto_id FROM kiosco_la_esquina.gold.dim_producto
     WHERE is_current = true
     GROUP BY producto_id HAVING COUNT(*) > 1
   ))                                                                        AS productos_con_2_vigentes,

  (SELECT COUNT(*) FROM (
     SELECT vendedor_id FROM kiosco_la_esquina.gold.dim_empleado
     WHERE is_current = true AND vendedor_id IS NOT NULL
     GROUP BY vendedor_id HAVING COUNT(*) > 1
   ))                                                                        AS vendedores_con_2_vigentes,

  (SELECT COUNT(*) FROM (
     SELECT row_hash FROM kiosco_la_esquina.gold.fact_ventas
     GROUP BY row_hash HAVING COUNT(*) > 1
   ))                                                                        AS row_hash_repetidos,

  (SELECT COUNT(*) FROM kiosco_la_esquina.silver.ventas)                     AS filas_silver,
  (SELECT COUNT(*) FROM kiosco_la_esquina.gold.fact_ventas)                  AS filas_fact,
  (SELECT COUNT(*) FROM kiosco_la_esquina.silver.ventas
   WHERE cantidad <= 0 AND estado_venta = 'aprobada')                        AS filas_excluidas;

SELECT * FROM controles_gold;

-- COMMAND ----------

SELECT assert_true(fks_huerfanas = 0,
  'Hay filas de fact_ventas sin match en alguna dimensión.') AS control_integridad
FROM controles_gold;

-- COMMAND ----------

SELECT assert_true(productos_con_2_vigentes = 0,
  'Hay productos con más de una versión vigente en dim_producto.') AS control_scd2_producto
FROM controles_gold;

-- COMMAND ----------

SELECT assert_true(vendedores_con_2_vigentes = 0,
  'Hay vendedores con más de una versión vigente en dim_empleado.') AS control_scd2_empleado
FROM controles_gold;

-- COMMAND ----------

SELECT assert_true(row_hash_repetidos = 0,
  'Hay row_hash repetidos en fact_ventas.') AS control_row_hash
FROM controles_gold;

-- COMMAND ----------

SELECT assert_true(filas_silver - filas_fact = filas_excluidas,
  'silver - fact no coincide con las filas excluidas: se perdieron o duplicaron ventas.') AS control_reconciliacion
FROM controles_gold;
