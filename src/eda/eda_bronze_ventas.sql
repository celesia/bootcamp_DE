-- Databricks notebook source
SELECT COUNT(*) AS total_filas FROM kiosco_la_esquina.bronze.ventas;

-- COMMAND ----------

DESCRIBE kiosco_la_esquina.bronze.ventas;

-- COMMAND ----------

SELECT * FROM kiosco_la_esquina.bronze.ventas LIMIT 20;

-- COMMAND ----------

SELECT
  ROUND(100.0 * COUNT_IF(vendedor_id IS NULL) / COUNT(*), 2)     AS pct_vendedor_id_nulo,
  ROUND(100.0 * COUNT_IF(vendedor_nombre IS NULL) / COUNT(*), 2) AS pct_vendedor_nombre_nulo,
  ROUND(100.0 * COUNT_IF(metodo_pago IS NULL) / COUNT(*), 2)     AS pct_metodo_pago_nulo
FROM kiosco_la_esquina.bronze.ventas;

-- COMMAND ----------

SELECT sucursal_nombre, COUNT(*) AS filas
FROM kiosco_la_esquina.bronze.ventas
GROUP BY sucursal_nombre
ORDER BY sucursal_nombre;

-- COMMAND ----------

SELECT estado_venta, COUNT(*) AS filas
FROM kiosco_la_esquina.bronze.ventas
GROUP BY estado_venta;

-- COMMAND ----------

SELECT metodo_pago, COUNT(*) AS filas
FROM kiosco_la_esquina.bronze.ventas
GROUP BY metodo_pago;

-- COMMAND ----------

SELECT DISTINCT precio_unitario
FROM kiosco_la_esquina.bronze.ventas
ORDER BY precio_unitario DESC
LIMIT 15;

-- COMMAND ----------

SELECT precio_unitario, COUNT(*) AS filas
FROM kiosco_la_esquina.bronze.ventas
WHERE NOT precio_unitario RLIKE '^[0-9]+\\.?[0-9]*$'
GROUP BY precio_unitario
ORDER BY filas DESC
LIMIT 15;

-- COMMAND ----------

SELECT ticket_id, producto_id, fecha_hora, cantidad, precio_unitario, COUNT(*) AS repeticiones
FROM kiosco_la_esquina.bronze.ventas
GROUP BY ticket_id, producto_id, fecha_hora, cantidad, precio_unitario
HAVING COUNT(*) > 1
ORDER BY repeticiones DESC
LIMIT 15;

-- COMMAND ----------

SELECT COUNT(*) AS filas_cantidad_invalida_aprobada
FROM kiosco_la_esquina.bronze.ventas
WHERE TRY_CAST(cantidad AS INT) <= 0 AND estado_venta = 'aprobada';

-- COMMAND ----------

WITH casteado AS (
  SELECT TRY_CAST(cantidad AS INT) AS cantidad_num FROM kiosco_la_esquina.bronze.ventas
)
SELECT cantidad_num, COUNT(*) AS filas
FROM casteado
WHERE cantidad_num > (SELECT PERCENTILE(cantidad_num, 0.99) FROM casteado)
GROUP BY cantidad_num
ORDER BY cantidad_num DESC;

-- COMMAND ----------

WITH por_producto AS (
  SELECT ticket_id, producto_id, COUNT(*) AS lineas
  FROM kiosco_la_esquina.bronze.ventas
  GROUP BY ticket_id, producto_id
)
SELECT lineas, COUNT(*) AS casos
FROM por_producto
GROUP BY lineas
ORDER BY lineas;

-- COMMAND ----------

SELECT
  DATE(fecha_hora)                                            AS dia,
  COUNT(*)                                                    AS lineas,
  SUM(COUNT(*)) OVER (ORDER BY DATE(fecha_hora))              AS lineas_acumuladas
FROM kiosco_la_esquina.bronze.ventas
GROUP BY DATE(fecha_hora)
ORDER BY dia;
