-- Databricks notebook source
-- MAGIC %md
-- MAGIC # ETL · Landing → Bronze
-- MAGIC
-- MAGIC SQL puro, sin PySpark. Lee los JSON del volumen con `read_files()` y
-- MAGIC los agrega a bronze con `INSERT INTO`.
-- MAGIC
-- MAGIC **El punto crítico:** el `schema` de `read_files()` declara **todas** las
-- MAGIC columnas como `STRING`, incluida `precio_unitario`. Sin esto, Databricks
-- MAGIC infiere el tipo mirando la mayoría de las filas y, al toparse con
-- MAGIC `"260,0"` en una columna que casi siempre es numérica, puede nulificar el
-- MAGIC valor sin ningún error visible. Con el schema forzado, todo se guarda como
-- MAGIC texto tal cual llegó. El casteo recién pasa en silver.
-- MAGIC
-- MAGIC Solo carga archivos que todavía no están en bronze (comparando la ruta
-- MAGIC del archivo contra `_source_file`) — así este notebook se puede correr
-- MAGIC solo, las veces que haga falta, sin depender de la corrida anterior.
-- MAGIC
-- MAGIC A propósito **no es idempotente** a nivel de fila: si se reprocesa el
-- MAGIC mismo archivo a mano borrando antes su rastro, bronze acumula filas
-- MAGIC repetidas. Limpiar eso es trabajo de silver, no de bronze.

-- COMMAND ----------

CREATE WIDGET TEXT run_id DEFAULT "manual";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Archivos de landing que todavía no están en bronze

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW archivos_nuevos AS
SELECT
  r.data,
  r._metadata.file_path AS source_file
FROM read_files(
  '/Volumes/kiosco_la_esquina/landing/raw_ventas/',
  format => 'json',
  multiLine => true,
  schema => '
    meta STRUCT<desde: STRING, hasta: STRING, filas: STRING>,
    data ARRAY<STRUCT<
      ticket_id: STRING,
      fecha_hora: STRING,
      sucursal_nombre: STRING,
      sucursal_ciudad: STRING,
      sucursal_departamento: STRING,
      vendedor_id: STRING,
      vendedor_nombre: STRING,
      producto_id: STRING,
      producto_nombre: STRING,
      categoria: STRING,
      precio_lista: STRING,
      precio_unitario: STRING,
      cantidad: STRING,
      descuento: STRING,
      metodo_pago: STRING,
      estado_venta: STRING
    >>
  '
) AS r
WHERE r._metadata.file_path NOT IN (
  SELECT _source_file FROM kiosco_la_esquina.bronze.ventas WHERE _source_file IS NOT NULL
);

-- COMMAND ----------

SELECT source_file, size(data) AS filas_en_el_archivo
FROM archivos_nuevos;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Insertar en bronze
-- MAGIC `LATERAL VIEW EXPLODE` convierte el array `data` de cada archivo en una fila por venta.

-- COMMAND ----------

INSERT INTO kiosco_la_esquina.bronze.ventas
SELECT
  fila.ticket_id,
  fila.fecha_hora,
  fila.sucursal_nombre,
  fila.sucursal_ciudad,
  fila.sucursal_departamento,
  fila.vendedor_id,
  fila.vendedor_nombre,
  fila.producto_id,
  fila.producto_nombre,
  fila.categoria,
  fila.precio_lista,
  fila.precio_unitario,
  fila.cantidad,
  fila.descuento,
  fila.metodo_pago,
  fila.estado_venta,
  source_file          AS _source_file,
  current_timestamp()  AS _ingest_timestamp,
  :run_id              AS _run_id
FROM archivos_nuevos
LATERAL VIEW EXPLODE(data) AS fila;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Actualizar el watermark
-- MAGIC
-- MAGIC Recién ahora, después de confirmar la carga — así un fallo entre landing
-- MAGIC y bronze nunca deja el watermark adelantado. La fecha sale del nombre de
-- MAGIC los archivos cargados en esta corrida (`ventas_<desde>_<hasta>.json`),
-- MAGIC tomando el `hasta` más reciente. Si esta corrida no cargó nada, el
-- MAGIC `WHERE m IS NOT NULL` deja la subconsulta vacía y el MERGE no toca nada.

-- COMMAND ----------

MERGE INTO kiosco_la_esquina.ops.watermark_ingesta AS destino
USING (
  SELECT 'api_ventas' AS fuente, m AS fecha_hasta_cargada
  FROM (
    SELECT MAX(DATE(regexp_extract(_source_file, '_([0-9]{4}-[0-9]{2}-[0-9]{2})\\.json$', 1))) AS m
    FROM kiosco_la_esquina.bronze.ventas
    WHERE _run_id = :run_id
  )
  WHERE m IS NOT NULL
) AS origen
ON destino.fuente = origen.fuente
WHEN MATCHED AND origen.fecha_hasta_cargada > destino.fecha_hasta_cargada THEN UPDATE SET
  destino.fecha_hasta_cargada = origen.fecha_hasta_cargada,
  destino.run_id = :run_id,
  destino.actualizado_en = current_timestamp()
WHEN NOT MATCHED THEN INSERT (fuente, fecha_hasta_cargada, run_id, actualizado_en)
  VALUES (origen.fuente, origen.fecha_hasta_cargada, :run_id, current_timestamp());

-- COMMAND ----------

SELECT * FROM kiosco_la_esquina.ops.watermark_ingesta;
