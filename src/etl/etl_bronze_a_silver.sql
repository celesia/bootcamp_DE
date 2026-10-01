-- Databricks notebook source
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
