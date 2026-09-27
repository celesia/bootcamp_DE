# Databricks notebook source
# MAGIC %md
# MAGIC # ETL · Bronze → Silver
# MAGIC
# MAGIC Limpia, tipa y deduplica. Es el único lugar del pipeline donde se castea
# MAGIC `precio_unitario` y se corrige `sucursal_nombre`/`descuento` — bronze nunca
# MAGIC transforma nada, y gold ya asume que silver viene limpio.
# MAGIC
# MAGIC Relee toda la tabla bronze en cada corrida (no solo lo nuevo): a esta
# MAGIC escala no pesa nada, y el `MERGE` final es idempotente igual — si una fila
# MAGIC ya existe en silver (mismo `row_hash`), simplemente no hace nada con ella.
# MAGIC En un proyecto con mucho más volumen, esto se optimizaría filtrando bronze
# MAGIC por `_ingest_timestamp` contra el último watermark de silver.

# COMMAND ----------

dbutils.widgets.text("run_id", "manual", "ID de la corrida")
RUN_ID = dbutils.widgets.get("run_id") or "manual"
CATALOGO = "kiosco_la_esquina"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limpieza y casteo
# MAGIC
# MAGIC - `sucursal_nombre`: `TRIM` + `INITCAP` normaliza `"KIOSCO LA ESQUINA CENTRO  "` a `"Kiosco La Esquina Centro"`.
# MAGIC - `precio_unitario`: reemplaza la coma decimal antes de castear (`"260,0"` -> `"260.0"` -> `260.0`).
# MAGIC - `descuento`: si viene `> 1`, es un porcentaje mal cargado como entero (`10` en vez de `0.10`) — se corrige dividiendo por 100.

# COMMAND ----------

df_limpio = spark.sql(f"""
    SELECT
      ticket_id,
      CAST(fecha_hora AS TIMESTAMP)                                         AS fecha_hora,
      INITCAP(TRIM(sucursal_nombre))                                        AS sucursal_nombre,
      sucursal_ciudad,
      sucursal_departamento,
      CAST(vendedor_id AS INT)                                              AS vendedor_id,
      vendedor_nombre,
      CAST(producto_id AS INT)                                              AS producto_id,
      producto_nombre,
      categoria,
      CAST(precio_lista AS DECIMAL(10, 2))                                  AS precio_lista,
      CAST(REPLACE(precio_unitario, ',', '.') AS DECIMAL(10, 2))            AS precio_unitario,
      CAST(cantidad AS INT)                                                 AS cantidad,
      CASE WHEN CAST(descuento AS DECIMAL(10, 4)) > 1
           THEN CAST(descuento AS DECIMAL(10, 4)) / 100
           ELSE CAST(descuento AS DECIMAL(10, 4))
      END                                                                    AS descuento,
      metodo_pago,
      estado_venta,
      _ingest_timestamp                                                     AS _bronze_ingest_timestamp
    FROM {CATALOGO}.bronze.ventas
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### `row_hash` y deduplicación
# MAGIC
# MAGIC El hash se calcula sobre los datos ya limpios, no sobre el string crudo de
# MAGIC bronze — así `"260,0"` y `260.0` (mismo valor, distinto formato) generan
# MAGIC el mismo hash en vez de contarse como filas distintas.
# MAGIC
# MAGIC La clave incluye `cantidad` y `precio_unitario` a propósito: dos líneas
# MAGIC del mismo producto en el mismo ticket son legítimas si difieren en algo
# MAGIC (cantidad, descuento aplicado); un duplicado real de una corrida repetida
# MAGIC es idéntico en todo, y ese es el que `ROW_NUMBER() = 1` descarta.

# COMMAND ----------

from pyspark.sql.functions import coalesce, col, concat_ws, current_timestamp, lit, md5, row_number
from pyspark.sql.window import Window

df_con_hash = df_limpio.withColumn(
    "row_hash",
    md5(concat_ws(
        "|",
        col("ticket_id"),
        col("producto_id").cast("string"),
        col("fecha_hora").cast("string"),
        col("cantidad").cast("string"),
        col("precio_unitario").cast("string"),
        coalesce(col("metodo_pago"), lit("SIN_METODO")),
    )),
)

ventana_dedup = Window.partitionBy("row_hash").orderBy(col("_bronze_ingest_timestamp").desc())

df_deduplicado = (
    df_con_hash
    .withColumn("_rn", row_number().over(ventana_dedup))
    .filter(col("_rn") == 1)
    .drop("_rn")
    .withColumn("_silver_updated_at", current_timestamp())
)

filas_antes = df_con_hash.count()
filas_despues = df_deduplicado.count()
print(f"Filas antes de deduplicar: {filas_antes}")
print(f"Filas después de deduplicar: {filas_despues} (se descartaron {filas_antes - filas_despues} duplicados)")

# COMMAND ----------

df_deduplicado.createOrReplaceTempView("silver_stage")

# COMMAND ----------

resultado_merge = spark.sql(f"""
    MERGE INTO {CATALOGO}.silver.ventas AS destino
    USING silver_stage AS origen
    ON destino.row_hash = origen.row_hash
    WHEN NOT MATCHED THEN INSERT *
""")

resultado_merge.display()
