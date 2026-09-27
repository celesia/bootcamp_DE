-- Databricks notebook source
-- MAGIC %md
-- MAGIC # EDA · bronze.ventas
-- MAGIC
-- MAGIC Los 7 pasos del curso, sobre bronze, **antes** de limpiar nada. El
-- MAGIC objetivo no es sacar conclusiones de negocio: es entender el estado real
-- MAGIC de los datos para documentarlos y prepararlos para silver. Corre suelto,
-- MAGIC no forma parte del Job.

-- COMMAND ----------

-- MAGIC %md ## 1. Exploración inicial

-- COMMAND ----------

SELECT COUNT(*) AS total_filas FROM kiosco_la_esquina.bronze.ventas;

-- COMMAND ----------

DESCRIBE kiosco_la_esquina.bronze.ventas;

-- COMMAND ----------

SELECT * FROM kiosco_la_esquina.bronze.ventas LIMIT 20;

-- COMMAND ----------

-- MAGIC %md ## 2. Valores nulos, en porcentaje (más fácil de leer que el número absoluto)

-- COMMAND ----------

SELECT
  ROUND(100.0 * COUNT_IF(vendedor_id IS NULL) / COUNT(*), 2)     AS pct_vendedor_id_nulo,
  ROUND(100.0 * COUNT_IF(vendedor_nombre IS NULL) / COUNT(*), 2) AS pct_vendedor_nombre_nulo,
  ROUND(100.0 * COUNT_IF(metodo_pago IS NULL) / COUNT(*), 2)     AS pct_metodo_pago_nulo
FROM kiosco_la_esquina.bronze.ventas;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Cardinalidad y categorías
-- MAGIC Debería haber 5 sucursales reales, pero van a aparecer más de 5 valores
-- MAGIC distintos por las variantes de mayúsculas y espacios. Eso es lo que silver
-- MAGIC normaliza con `TRIM` + `INITCAP`.

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

-- MAGIC %md
-- MAGIC ## 4. Estadísticas y el problema de comparación bit a bit
-- MAGIC `precio_unitario` está en STRING: ordenarlo tal cual compara texto, no
-- MAGIC número (`"9"` queda arriba de `"80"` porque compara carácter por
-- MAGIC carácter). Hay que castear antes de sacar cualquier conclusión numérica.

-- COMMAND ----------

-- Sin castear: orden "raro", compara como texto
SELECT DISTINCT precio_unitario
FROM kiosco_la_esquina.bronze.ventas
ORDER BY precio_unitario DESC
LIMIT 15;

-- COMMAND ----------

-- Filas con coma decimal en vez de punto: lo que silver tiene que arreglar
SELECT precio_unitario, COUNT(*) AS filas
FROM kiosco_la_esquina.bronze.ventas
WHERE NOT precio_unitario RLIKE '^[0-9]+\\.?[0-9]*$'
GROUP BY precio_unitario
ORDER BY filas DESC
LIMIT 15;

-- COMMAND ----------

-- MAGIC %md ## 5. Calidad: duplicados y outliers

-- COMMAND ----------

-- Duplicados exactos: el reintento de ingesta que simula la API a propósito
SELECT ticket_id, producto_id, fecha_hora, cantidad, precio_unitario, COUNT(*) AS repeticiones
FROM kiosco_la_esquina.bronze.ventas
GROUP BY ticket_id, producto_id, fecha_hora, cantidad, precio_unitario
HAVING COUNT(*) > 1
ORDER BY repeticiones DESC
LIMIT 15;

-- COMMAND ----------

-- cantidad <= 0 en ventas "aprobada": dato sucio, no una devolución real
SELECT COUNT(*) AS filas_cantidad_invalida_aprobada
FROM kiosco_la_esquina.bronze.ventas
WHERE TRY_CAST(cantidad AS INT) <= 0 AND estado_venta = 'aprobada';

-- COMMAND ----------

-- Outliers de cantidad por encima del percentil 99
WITH casteado AS (
  SELECT TRY_CAST(cantidad AS INT) AS cantidad_num FROM kiosco_la_esquina.bronze.ventas
)
SELECT cantidad_num, COUNT(*) AS filas
FROM casteado
WHERE cantidad_num > (SELECT PERCENTILE(cantidad_num, 0.99) FROM casteado)
GROUP BY cantidad_num
ORDER BY cantidad_num DESC;

-- COMMAND ----------

-- MAGIC %md ## 6. Análisis con CTE y window functions

-- COMMAND ----------

-- Tickets con más de una línea del mismo producto: confirma que la clave de
-- deduplicación de silver no puede ser (ticket_id, producto_id) sola.
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

-- Ventas por día con un acumulado móvil (window function)
SELECT
  DATE(fecha_hora)                                            AS dia,
  COUNT(*)                                                    AS lineas,
  SUM(COUNT(*)) OVER (ORDER BY DATE(fecha_hora))              AS lineas_acumuladas
FROM kiosco_la_esquina.bronze.ventas
GROUP BY DATE(fecha_hora)
ORDER BY dia;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. Conclusiones (a documentar antes de pasar a silver)
-- MAGIC
-- MAGIC - `sucursal_nombre` necesita `TRIM` + `INITCAP`.
-- MAGIC - `precio_unitario` necesita `REPLACE(',', '.')` antes de `CAST(... AS DECIMAL)`.
-- MAGIC - `descuento` tiene valores cargados como entero (10 en vez de 0.10): corregir si es mayor a 1.
-- MAGIC - `(ticket_id, producto_id)` no es clave única. La deduplicación de silver usa el hash de la línea completa.
-- MAGIC - Hay duplicados exactos de fila completa: se resuelven con `ROW_NUMBER()` en silver.
-- MAGIC - `cantidad <= 0` en ventas "aprobada" se conserva en silver y se excluye recién en gold.
-- MAGIC - Hay outliers de cantidad y de precio: quedan en silver (son datos reales de la fuente) y se pueden filtrar en la capa semántica si hace falta.
