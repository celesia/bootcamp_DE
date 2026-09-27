-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Calidad · Silver
-- MAGIC
-- MAGIC Corre después del `MERGE` a silver, antes de tocar gold. Primero muestra
-- MAGIC el resultado de todos los controles en una tabla. Después, un
-- MAGIC `assert_true()` por control: el que no se cumpla corta la tarea del Job.

-- COMMAND ----------

-- MAGIC %md ### Resultado de los controles

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW controles_silver AS
SELECT
  -- Mismo row_hash en más de una fila: el MERGE no deduplicó bien
  (SELECT COUNT(*) FROM (
     SELECT row_hash FROM kiosco_la_esquina.silver.ventas
     GROUP BY row_hash HAVING COUNT(*) > 1
   ))                                                                             AS row_hash_repetidos,
  -- Ventas sin producto, sucursal o fecha
  COUNT_IF(producto_id IS NULL OR sucursal_nombre IS NULL OR fecha_hora IS NULL) AS ventas_sin_clave,
  -- Fechas fuera del rango del negocio: antes de la apertura o en el futuro
  COUNT_IF(fecha_hora < TIMESTAMP'2026-09-01' OR fecha_hora > current_timestamp()) AS fechas_fuera_de_rango,
  -- Precios que TRY_CAST no pudo convertir: aparecería un formato nuevo
  COUNT_IF(precio_unitario IS NULL)                                               AS precios_sin_convertir,
  COUNT(*)                                                                        AS filas
FROM kiosco_la_esquina.silver.ventas;

SELECT * FROM controles_silver;

-- COMMAND ----------

-- MAGIC %md ### Gates

-- COMMAND ----------

SELECT assert_true(row_hash_repetidos = 0,
  'Hay row_hash repetidos en silver.ventas.') AS control_row_hash
FROM controles_silver;

-- COMMAND ----------

SELECT assert_true(ventas_sin_clave = 0,
  'Hay ventas sin producto, sucursal o fecha en silver.ventas.') AS control_claves
FROM controles_silver;

-- COMMAND ----------

SELECT assert_true(fechas_fuera_de_rango = 0,
  'Hay ventas con fecha antes del 2026-09-01 o en el futuro.') AS control_fechas
FROM controles_silver;

-- COMMAND ----------

-- Se tolera hasta 1% de precios sin convertir; más que eso indica un formato nuevo
SELECT assert_true(precios_sin_convertir < 0.01 * filas,
  'Más del 1% de los precios no se pudo convertir a número: revisar la limpieza.') AS control_precios
FROM controles_silver;
