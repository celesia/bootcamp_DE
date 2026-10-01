-- Databricks notebook source
CREATE OR REPLACE TEMPORARY VIEW fact_ventas_stage AS
SELECT
  s.row_hash,
  s.ticket_id,
  CAST(date_format(s.fecha_hora, 'yyyyMMdd') AS INT)                            AS tiempo_sk,
  md5(s.sucursal_nombre)                                                        AS sucursal_sk,
  p.producto_sk,
  COALESCE(e.empleado_sk, '-1')                                                 AS empleado_sk,
  md5(concat_ws('|', COALESCE(s.metodo_pago, 'no_informado'), s.estado_venta))  AS transaccion_sk,
  s.cantidad,
  s.precio_unitario,
  s.descuento,
  CAST(s.cantidad * s.precio_unitario AS DECIMAL(12, 2))                        AS venta_neta
FROM kiosco_la_esquina.silver.ventas s
LEFT JOIN kiosco_la_esquina.gold.dim_producto p
  ON s.producto_id = p.producto_id
 AND s.fecha_hora BETWEEN p.valid_from AND p.valid_to
LEFT JOIN kiosco_la_esquina.gold.dim_empleado e
  ON s.vendedor_id = e.vendedor_id
 AND s.fecha_hora BETWEEN e.valid_from AND e.valid_to
WHERE NOT (s.cantidad <= 0 AND s.estado_venta = 'aprobada');

-- COMMAND ----------

SELECT assert_true(
  (SELECT COUNT(*) FROM fact_ventas_stage WHERE producto_sk IS NULL) = 0,
  'Hay ventas sin versión vigente en dim_producto. Cargar dim_producto antes que la fact.'
) AS gate_producto;

-- COMMAND ----------

MERGE INTO kiosco_la_esquina.gold.fact_ventas AS destino
USING fact_ventas_stage AS origen
ON destino.row_hash = origen.row_hash
WHEN NOT MATCHED THEN INSERT (
  row_hash, ticket_id, tiempo_sk, sucursal_sk, producto_sk, empleado_sk,
  transaccion_sk, cantidad, precio_unitario, descuento, venta_neta, _gold_loaded_at
) VALUES (
  origen.row_hash, origen.ticket_id, origen.tiempo_sk, origen.sucursal_sk, origen.producto_sk,
  origen.empleado_sk, origen.transaccion_sk, origen.cantidad, origen.precio_unitario,
  origen.descuento, origen.venta_neta, current_timestamp()
);

-- COMMAND ----------

SELECT
  (SELECT COUNT(*) FROM kiosco_la_esquina.gold.fact_ventas) AS filas_en_fact,
  (SELECT COUNT(*) FROM kiosco_la_esquina.silver.ventas
   WHERE cantidad <= 0 AND estado_venta = 'aprobada')      AS excluidas_por_cantidad_invalida;
