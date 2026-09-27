# Databricks notebook source
# MAGIC %md
# MAGIC # EDA · bronze.ventas
# MAGIC
# MAGIC Los 7 pasos del curso, sobre bronze, **antes** de limpiar nada — el
# MAGIC objetivo acá no es sacar conclusiones de negocio, es entender el estado
# MAGIC real de los datos para poder documentarlos y prepararlos para silver.
# MAGIC Corre suelto, no forma parte del Job — es exploración, no un paso del
# MAGIC pipeline productivo.

# COMMAND ----------

CATALOGO = "kiosco_la_esquina"

# COMMAND ----------

# MAGIC %md ## 1. Exploración inicial

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_filas FROM kiosco_la_esquina.bronze.ventas;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE kiosco_la_esquina.bronze.ventas;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM kiosco_la_esquina.bronze.ventas LIMIT 20;

# COMMAND ----------

# MAGIC %md ## 2. Valores nulos, en porcentaje (más fácil de leer que el número absoluto)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ROUND(100.0 * SUM(CASE WHEN vendedor_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_vendedor_id_nulo,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN vendedor_nombre IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_vendedor_nombre_nulo,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN metodo_pago IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_metodo_pago_nulo
# MAGIC FROM kiosco_la_esquina.bronze.ventas;

# COMMAND ----------

# MAGIC %md ## 3. Cardinalidad y categorías — acá aparece la suciedad de `sucursal_nombre`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Debería haber 5 sucursales reales, pero van a aparecer más de 5 valores
# MAGIC -- distintos por las variantes de mayúsculas/espacios — esto es lo que
# MAGIC -- silver tiene que normalizar con TRIM + UPPER/INITCAP.
# MAGIC SELECT sucursal_nombre, COUNT(*) AS filas
# MAGIC FROM kiosco_la_esquina.bronze.ventas
# MAGIC GROUP BY sucursal_nombre
# MAGIC ORDER BY sucursal_nombre;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT estado_venta, COUNT(*) AS filas
# MAGIC FROM kiosco_la_esquina.bronze.ventas
# MAGIC GROUP BY estado_venta;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT metodo_pago, COUNT(*) AS filas
# MAGIC FROM kiosco_la_esquina.bronze.ventas
# MAGIC GROUP BY metodo_pago;

# COMMAND ----------

# MAGIC %md ## 4. Estadísticas y problema de comparación bit a bit
# MAGIC
# MAGIC `precio_unitario` está en STRING — ordenarlo tal cual compara texto, no
# MAGIC número (`"9"` > `"80"` porque compara carácter por carácter). Hay que
# MAGIC castear antes de sacar cualquier conclusión numérica.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sin castear: orden "raro", compara como texto
# MAGIC SELECT DISTINCT precio_unitario
# MAGIC FROM kiosco_la_esquina.bronze.ventas
# MAGIC ORDER BY precio_unitario DESC
# MAGIC LIMIT 15;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Filas donde precio_unitario NO matchea un número simple (tiene coma
# MAGIC -- en vez de punto) — esto es exactamente lo que hay que arreglar en silver.
# MAGIC SELECT precio_unitario, COUNT(*) AS filas
# MAGIC FROM kiosco_la_esquina.bronze.ventas
# MAGIC WHERE NOT precio_unitario RLIKE '^[0-9]+\\.?[0-9]*$'
# MAGIC GROUP BY precio_unitario
# MAGIC ORDER BY filas DESC
# MAGIC LIMIT 15;

# COMMAND ----------

# MAGIC %md ## 5. Detección de calidad: duplicados y outliers

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Duplicados exactos (fila completa repetida) — el reintento de ingesta
# MAGIC -- simulado a propósito por la API.
# MAGIC SELECT ticket_id, producto_id, fecha_hora, cantidad, precio_unitario, COUNT(*) AS repeticiones
# MAGIC FROM kiosco_la_esquina.bronze.ventas
# MAGIC GROUP BY ticket_id, producto_id, fecha_hora, cantidad, precio_unitario
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY repeticiones DESC
# MAGIC LIMIT 15;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- cantidad=0 en ventas "aprobada": dato sucio, no una devolución real
# MAGIC SELECT COUNT(*) AS filas_cantidad_cero_aprobada
# MAGIC FROM kiosco_la_esquina.bronze.ventas
# MAGIC WHERE CAST(cantidad AS INT) <= 0 AND estado_venta = 'aprobada';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Outliers de cantidad, ya casteada, fuera del percentil 99
# MAGIC WITH casteado AS (
# MAGIC   SELECT CAST(cantidad AS INT) AS cantidad_num FROM kiosco_la_esquina.bronze.ventas
# MAGIC )
# MAGIC SELECT cantidad_num, COUNT(*) AS filas
# MAGIC FROM casteado
# MAGIC WHERE cantidad_num > (SELECT PERCENTILE(cantidad_num, 0.99) FROM casteado)
# MAGIC GROUP BY cantidad_num
# MAGIC ORDER BY cantidad_num DESC;

# COMMAND ----------

# MAGIC %md ## 6. Análisis avanzado con CTE y window functions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tickets con más de una línea del mismo producto — confirma que la
# MAGIC -- clave de deduplicación de silver no puede ser (ticket_id, producto_id)
# MAGIC -- sola, tal como se documentó en el plan de arquitectura.
# MAGIC WITH por_producto AS (
# MAGIC   SELECT ticket_id, producto_id, COUNT(*) AS lineas
# MAGIC   FROM kiosco_la_esquina.bronze.ventas
# MAGIC   GROUP BY ticket_id, producto_id
# MAGIC )
# MAGIC SELECT lineas, COUNT(*) AS cuantos_casos
# MAGIC FROM por_producto
# MAGIC GROUP BY lineas
# MAGIC ORDER BY lineas;

# COMMAND ----------

# MAGIC %md ## 7. Conclusiones (documentar acá antes de pasar a silver)
# MAGIC
# MAGIC - `sucursal_nombre` necesita `TRIM` + normalización de mayúsculas.
# MAGIC - `precio_unitario` necesita `REPLACE(',', '.')` antes de `CAST(... AS DECIMAL)`.
# MAGIC - `descuento` tiene valores mal cargados como enteros (10 en vez de 0.10) — corregir si `descuento > 1`.
# MAGIC - `(ticket_id, producto_id)` NO es una clave única — confirmado arriba. La clave de dedup de silver usa el hash completo de la línea de negocio.
# MAGIC - Hay duplicados exactos de fila completa — se resuelven con `ROW_NUMBER()` en `etl_bronze_a_silver.py`.
# MAGIC - `cantidad = 0` en ventas "aprobada" se conserva en silver, se excluye recién en gold (fact_ventas).
