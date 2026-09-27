# Databricks notebook source
# MAGIC %md
# MAGIC # ETL · Landing → Bronze
# MAGIC
# MAGIC Lee los archivos JSON de landing que todavía no están en bronze (comparado
# MAGIC por `_source_file`, no por lo que haya hecho la corrida anterior — así este
# MAGIC notebook se puede correr solo, las veces que haga falta) y los agrega a
# MAGIC bronze con `INSERT INTO`.
# MAGIC
# MAGIC **El punto crítico:** se fuerza un schema con **todas** las columnas como
# MAGIC `StringType`, incluida `precio_unitario`. Sin esto, Spark infiere el schema
# MAGIC del JSON y, al toparse con `"260,0"` (string) en una columna que en la
# MAGIC mayoría de las filas es numérica, puede nulificar el valor o inferir tipos
# MAGIC distintos entre archivos — perdiendo datos sin ningún error visible. Con el
# MAGIC schema forzado, cualquier valor se guarda como texto tal cual llegó, sin
# MAGIC excepción. El casteo real recién pasa en `etl_bronze_a_silver.py`.
# MAGIC
# MAGIC A propósito **no es idempotente**: si se reprocesa el mismo archivo más de
# MAGIC una vez a mano, bronze acumula esas filas repetidas. Limpiar eso es trabajo
# MAGIC de silver (`MERGE` + dedup por hash), no de bronze.

# COMMAND ----------

dbutils.widgets.text("run_id", "manual", "ID de la corrida")
RUN_ID = dbutils.widgets.get("run_id") or "manual"

CATALOGO = "kiosco_la_esquina"
VOLUMEN = f"/Volumes/{CATALOGO}/landing/raw_ventas"

# COMMAND ----------

from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from pyspark.sql.functions import col, current_timestamp, explode, input_file_name, lit

# Todas las columnas de negocio en STRING, sin excepción — ese es el punto
# de esta celda. precio_unitario podría parecer que "debería" ser un
# número, pero justamente por eso hay que forzarlo: si se lo deja inferir,
# Spark decide el tipo mirando la mayoría de las filas y rompe con las
# que vienen sucias.
schema_fila = StructType([
    StructField("ticket_id", StringType()),
    StructField("fecha_hora", StringType()),
    StructField("sucursal_nombre", StringType()),
    StructField("sucursal_ciudad", StringType()),
    StructField("sucursal_departamento", StringType()),
    StructField("vendedor_id", StringType()),
    StructField("vendedor_nombre", StringType()),
    StructField("producto_id", StringType()),
    StructField("producto_nombre", StringType()),
    StructField("categoria", StringType()),
    StructField("precio_lista", StringType()),
    StructField("precio_unitario", StringType()),
    StructField("cantidad", StringType()),
    StructField("descuento", StringType()),
    StructField("metodo_pago", StringType()),
    StructField("estado_venta", StringType()),
])

schema_respuesta = StructType([
    StructField("meta", StructType([
        StructField("desde", StringType()),
        StructField("hasta", StringType()),
        StructField("filas", StringType()),
    ])),
    StructField("data", ArrayType(schema_fila)),
])

# COMMAND ----------

# ¿Qué archivos de landing todavía no están en bronze?
archivos_en_volumen = {f.path for f in dbutils.fs.ls(VOLUMEN)}

archivos_en_bronze = {
    r["_source_file"]
    for r in spark.sql(f"SELECT DISTINCT _source_file FROM {CATALOGO}.bronze.ventas").collect()
}

archivos_nuevos = sorted(archivos_en_volumen - archivos_en_bronze)

if not archivos_nuevos:
    print("No hay archivos nuevos de landing para cargar a bronze.")
    dbutils.notebook.exit("sin_archivos_nuevos")

print(f"{len(archivos_nuevos)} archivo(s) nuevo(s) para cargar:")
for a in archivos_nuevos:
    print(f"  {a}")

# COMMAND ----------

df_crudo = (
    spark.read.schema(schema_respuesta)
    .json(archivos_nuevos)
    .withColumn("_source_file", input_file_name())
)

df_bronze = (
    df_crudo
    .select("_source_file", explode(col("data")).alias("fila"))
    .select(
        "fila.*",
        "_source_file",
    )
    .withColumn("_ingest_timestamp", current_timestamp())
    .withColumn("_run_id", lit(RUN_ID))
)

filas_a_insertar = df_bronze.count()
print(f"Filas a insertar en bronze: {filas_a_insertar}")

# Sanity check al correrlo por primera vez: confirmar que _source_file tiene
# pinta de ruta real (algo como dbfs:/Volumes/kiosco_la_esquina/landing/...).
# Si sale vacío o distinto, algo no matchea entre dbutils.fs.ls() y
# input_file_name() y hay que revisar antes de seguir.
df_bronze.select("_source_file").distinct().show(truncate=False)

# COMMAND ----------

df_bronze.write.mode("append").saveAsTable(f"{CATALOGO}.bronze.ventas")
print(f"Insertadas {filas_a_insertar} filas en {CATALOGO}.bronze.ventas")

# COMMAND ----------

# MAGIC %md
# MAGIC Actualizar el watermark recién ahora, después de confirmar la carga —
# MAGIC así un fallo entre landing y bronze nunca deja el watermark adelantado
# MAGIC respecto a lo realmente persistido. La fecha se saca del nombre de los
# MAGIC archivos cargados (`ventas_<desde>_<hasta>.json`), tomando el `hasta` más
# MAGIC reciente.

# COMMAND ----------

import re

fechas_hasta = [
    re.search(r"_(\d{4}-\d{2}-\d{2})\.json$", a).group(1)
    for a in archivos_nuevos
    if re.search(r"_(\d{4}-\d{2}-\d{2})\.json$", a)
]
nuevo_watermark = max(fechas_hasta)

spark.sql(f"""
    MERGE INTO {CATALOGO}.ops.watermark_ingesta AS d
    USING (SELECT 'api_ventas' AS fuente,
                  DATE('{nuevo_watermark}') AS fecha_hasta_cargada,
                  '{RUN_ID}' AS run_id) AS o
    ON d.fuente = o.fuente
    WHEN MATCHED AND o.fecha_hasta_cargada > d.fecha_hasta_cargada
      THEN UPDATE SET d.fecha_hasta_cargada = o.fecha_hasta_cargada,
                       d.run_id = o.run_id,
                       d.actualizado_en = current_timestamp()
    WHEN NOT MATCHED
      THEN INSERT (fuente, fecha_hasta_cargada, run_id, actualizado_en)
           VALUES (o.fuente, o.fecha_hasta_cargada, o.run_id, current_timestamp())
""")

print(f"Watermark actualizado hasta {nuevo_watermark}")
