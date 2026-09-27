-- Databricks notebook source
-- MAGIC %md
-- MAGIC # ETL · gold.dim_producto (SCD Tipo 2 sobre `precio_lista`)
-- MAGIC
-- MAGIC Reconstruye el historial completo de versiones de precio a partir de todo
-- MAGIC silver (no solo lo nuevo de esta corrida). Hace falta porque una sola
-- MAGIC corrida de backfill puede traer varios cambios de precio juntos, y el
-- MAGIC patrón de "cerrar la vigente + insertar la nueva" del curso está pensado
-- MAGIC para un cambio por vez.
-- MAGIC
-- MAGIC **Paso 1 (MERGE):** si la versión que hoy figura como vigente en la
-- MAGIC dimensión dejó de serlo, se la cierra con el `valid_to` que ya viene
-- MAGIC calculado en la reconstrucción — no se asume que el cambio nuevo es el único.
-- MAGIC
-- MAGIC **Paso 2 (INSERT):** se agregan todas las versiones que todavía no existen
-- MAGIC en la dimensión (comparando por `producto_sk`), sea una o varias.
-- MAGIC
-- MAGIC **Limitación documentada:** el `valid_from` de cada versión es la fecha de
-- MAGIC la primera *venta* a ese precio. Si un producto no se vendió el día que
-- MAGIC subió, la versión se detecta recién con la primera venta posterior. Solo
-- MAGIC vemos el precio a través de las ventas, no hay un feed de catálogo aparte.

-- COMMAND ----------

-- MAGIC %md ### Reconstruir el historial completo de versiones desde silver

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW dim_producto_versiones AS
WITH precios_por_dia AS (
  SELECT DISTINCT producto_id, producto_nombre, categoria, precio_lista, DATE(fecha_hora) AS fecha
  FROM kiosco_la_esquina.silver.ventas
),
marcado_cambio AS (
  SELECT *,
    LAG(precio_lista) OVER (PARTITION BY producto_id ORDER BY fecha) AS precio_anterior
  FROM precios_por_dia
),
inicios_de_version AS (
  SELECT producto_id, producto_nombre, categoria, precio_lista, fecha AS valid_from
  FROM marcado_cambio
  WHERE precio_anterior IS NULL OR precio_anterior != precio_lista
),
con_fin AS (
  SELECT *,
    LEAD(valid_from) OVER (PARTITION BY producto_id ORDER BY valid_from) AS siguiente_inicio
  FROM inicios_de_version
)
SELECT
  -- El hash se ancla a CAST(valid_from AS DATE) explícitamente: la versión
  -- empieza un día (esa es su granularidad real), y así el hash no depende
  -- de si en este punto valid_from es DATE o ya fue casteado a TIMESTAMP.
  md5(concat_ws('|', CAST(producto_id AS STRING), CAST(CAST(valid_from AS DATE) AS STRING))) AS producto_sk,
  producto_id,
  producto_nombre,
  categoria,
  precio_lista,
  CAST(valid_from AS TIMESTAMP) AS valid_from,
  CASE WHEN siguiente_inicio IS NULL THEN TIMESTAMP'9999-12-31'
       ELSE CAST(siguiente_inicio AS TIMESTAMP) - INTERVAL 1 SECOND
  END AS valid_to,
  siguiente_inicio IS NULL AS is_current
FROM con_fin;

-- COMMAND ----------

SELECT producto_id, producto_nombre, COUNT(*) AS versiones
FROM dim_producto_versiones
GROUP BY producto_id, producto_nombre
ORDER BY producto_id;

-- COMMAND ----------

-- MAGIC %md ### Paso 1: cerrar la versión vigente si el historial dice que ya no lo es

-- COMMAND ----------

MERGE INTO kiosco_la_esquina.gold.dim_producto AS destino
USING dim_producto_versiones AS origen
ON  destino.producto_id = origen.producto_id
AND destino.valid_from  = origen.valid_from
AND destino.is_current  = true
WHEN MATCHED AND origen.is_current = false THEN UPDATE SET
  destino.valid_to   = origen.valid_to,
  destino.is_current = false;

-- COMMAND ----------

-- MAGIC %md ### Paso 2: insertar las versiones que todavía no existen

-- COMMAND ----------

INSERT INTO kiosco_la_esquina.gold.dim_producto
  (producto_sk, producto_id, producto_nombre, categoria, precio_lista, valid_from, valid_to, is_current)
SELECT v.producto_sk, v.producto_id, v.producto_nombre, v.categoria, v.precio_lista,
       v.valid_from, v.valid_to, v.is_current
FROM dim_producto_versiones v
LEFT ANTI JOIN kiosco_la_esquina.gold.dim_producto d
  ON v.producto_sk = d.producto_sk;

-- COMMAND ----------

-- MAGIC %md ### Verificación: esta consulta tiene que volver vacía (nunca más de una versión vigente por producto)

-- COMMAND ----------

SELECT producto_id, COUNT(*) AS versiones_vigentes
FROM kiosco_la_esquina.gold.dim_producto
WHERE is_current = true
GROUP BY producto_id
HAVING COUNT(*) > 1;
