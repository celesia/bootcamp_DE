-- Databricks notebook source
-- MAGIC %md
-- MAGIC # ETL · Bronze → Silver
-- MAGIC
-- MAGIC Limpia, tipa y deduplica, en un solo `MERGE`. Es el único lugar del
-- MAGIC pipeline donde se castea `precio_unitario` y se corrigen `sucursal_nombre`
-- MAGIC y `descuento` — bronze nunca transforma nada, y gold ya asume que silver
-- MAGIC viene limpio.
-- MAGIC
-- MAGIC Se arma por pasos con vistas temporales (una por transformación), como
-- MAGIC se enseñó en clase: así se puede hacer `SELECT *` sobre cada paso para
-- MAGIC validarlo sin repetir toda la lógica anterior.
-- MAGIC
-- MAGIC Relee toda bronze en cada corrida. A esta escala no pesa nada, y el
-- MAGIC `MERGE` es idempotente igual: si una fila ya existe en silver (mismo
-- MAGIC `row_hash`), no hace nada con ella.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Paso 1: limpieza y casteo
-- MAGIC
-- MAGIC - `sucursal_nombre`: `TRIM` + `INITCAP` normaliza `"KIOSCO LA ESQUINA CENTRO  "` a `"Kiosco La Esquina Centro"`.
-- MAGIC - `precio_unitario`: se reemplaza la coma decimal antes de castear (`"260,0"` → `260.0`).
-- MAGIC - `descuento`: si viene mayor a 1, es un porcentaje mal cargado como entero (`10` en vez de `0.10`) y se divide por 100.
-- MAGIC
-- MAGIC Se usa `TRY_CAST` y no `CAST`: serverless corre en modo ANSI, donde un
-- MAGIC `CAST` que no puede convertir un valor tira error y corta toda la
-- MAGIC consulta. `TRY_CAST` devuelve NULL en ese caso, y el control de calidad de
-- MAGIC silver cuenta esos NULL para avisar si apareció un formato nuevo.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW silver_paso1_limpio AS
SELECT
  ticket_id,
  TRY_CAST(fecha_hora AS TIMESTAMP)                                 AS fecha_hora,
  INITCAP(TRIM(sucursal_nombre))                                    AS sucursal_nombre,
  sucursal_ciudad,
  sucursal_departamento,
  TRY_CAST(vendedor_id AS INT)                                      AS vendedor_id,
  vendedor_nombre,
  TRY_CAST(producto_id AS INT)                                      AS producto_id,
  producto_nombre,
  categoria,
  TRY_CAST(precio_lista AS DECIMAL(10, 2))                          AS precio_lista,
  TRY_CAST(REPLACE(precio_unitario, ',', '.') AS DECIMAL(10, 2))    AS precio_unitario,
  TRY_CAST(cantidad AS INT)                                         AS cantidad,
  CASE
    WHEN TRY_CAST(descuento AS DECIMAL(10, 4)) > 1 THEN TRY_CAST(descuento AS DECIMAL(10, 4)) / 100
    ELSE TRY_CAST(descuento AS DECIMAL(10, 4))
  END                                                                AS descuento,
  metodo_pago,
  estado_venta,
  _ingest_timestamp                                              AS _bronze_ingest_timestamp
FROM kiosco_la_esquina.bronze.ventas;

-- COMMAND ----------

SELECT * FROM silver_paso1_limpio LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Paso 2: `row_hash`
-- MAGIC
-- MAGIC Se calcula sobre los datos ya limpios, no sobre el string crudo de bronze —
-- MAGIC así `"260,0"` y `260.0` (mismo valor, distinto formato) dan el mismo hash.
-- MAGIC
-- MAGIC Incluye `cantidad` y `precio_unitario` a propósito: dos líneas del mismo
-- MAGIC producto en el mismo ticket son legítimas si difieren en algo; un
-- MAGIC duplicado real de ingesta es idéntico en todo.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW silver_paso2_hash AS
SELECT
  md5(concat_ws('|',
    ticket_id,
    CAST(producto_id AS STRING),
    CAST(fecha_hora AS STRING),
    CAST(cantidad AS STRING),
    CAST(precio_unitario AS STRING),
    COALESCE(metodo_pago, 'SIN_METODO')
  )) AS row_hash,
  *
FROM silver_paso1_limpio;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Paso 3: deduplicar con `ROW_NUMBER()`
-- MAGIC Se queda con la versión más reciente de cada `row_hash`.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW silver_paso3_dedup AS
SELECT * EXCEPT (rn)
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY row_hash ORDER BY _bronze_ingest_timestamp DESC) AS rn
  FROM silver_paso2_hash
)
WHERE rn = 1;

-- COMMAND ----------

SELECT
  (SELECT COUNT(*) FROM silver_paso2_hash)  AS filas_antes_de_deduplicar,
  (SELECT COUNT(*) FROM silver_paso3_dedup) AS filas_despues_de_deduplicar;

-- COMMAND ----------

-- MAGIC %md ### Paso 4: `MERGE` a silver

-- COMMAND ----------

MERGE INTO kiosco_la_esquina.silver.ventas AS destino
USING (
  SELECT
    row_hash, ticket_id, producto_id, fecha_hora, sucursal_nombre, sucursal_ciudad,
    sucursal_departamento, vendedor_id, vendedor_nombre, producto_nombre, categoria,
    precio_lista, precio_unitario, cantidad, descuento, metodo_pago, estado_venta,
    _bronze_ingest_timestamp,
    current_timestamp() AS _silver_updated_at
  FROM silver_paso3_dedup
) AS origen
ON destino.row_hash = origen.row_hash
WHEN NOT MATCHED THEN INSERT *;
