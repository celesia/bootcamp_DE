# Databricks notebook source
# MAGIC %md
# MAGIC # Calidad · Silver
# MAGIC
# MAGIC Corre después del `MERGE` a silver, antes de tocar gold.

# COMMAND ----------

dbutils.widgets.text("run_id", "manual", "ID de la corrida")
RUN_ID = dbutils.widgets.get("run_id") or "manual"
CATALOGO = "kiosco_la_esquina"
APERTURA = "2026-09-01"

# COMMAND ----------

resultados = []  # (chequeo, descripcion, severidad, filas_afectadas, ok)

# COMMAND ----------

# MAGIC %md ### Unicidad de `row_hash`

# COMMAND ----------

dup = spark.sql(f"""
    SELECT COUNT(*) AS extra
    FROM (
      SELECT row_hash, COUNT(*) AS n
      FROM {CATALOGO}.silver.ventas
      GROUP BY row_hash
      HAVING COUNT(*) > 1
    )
""").first()["extra"]

resultados.append(("row_hash_unico", "row_hash no se repite en silver", "FAIL", dup, dup == 0))

# COMMAND ----------

# MAGIC %md ### Nulls en las FKs de negocio (no puede haber una venta sin producto, sucursal o fecha)

# COMMAND ----------

nulls_fk = spark.sql(f"""
    SELECT COUNT(*) AS filas
    FROM {CATALOGO}.silver.ventas
    WHERE producto_id IS NULL OR sucursal_nombre IS NULL OR fecha_hora IS NULL
""").first()["filas"]

resultados.append(("fks_no_nulas", "producto_id/sucursal_nombre/fecha_hora nunca nulos", "FAIL", nulls_fk, nulls_fk == 0))

# COMMAND ----------

# MAGIC %md ### Fechas dentro del rango válido del negocio

# COMMAND ----------

fuera_de_rango = spark.sql(f"""
    SELECT COUNT(*) AS filas
    FROM {CATALOGO}.silver.ventas
    WHERE fecha_hora < TIMESTAMP('{APERTURA}') OR fecha_hora > CURRENT_TIMESTAMP()
""").first()["filas"]

resultados.append(("fechas_validas", f"fecha_hora entre {APERTURA} y hoy", "FAIL", fuera_de_rango, fuera_de_rango == 0))

# COMMAND ----------

# MAGIC %md ### `precio_unitario` nulo tras el cast (indicaría un formato que la limpieza no contempló)

# COMMAND ----------

precio_nulo = spark.sql(f"""
    SELECT COUNT(*) AS filas FROM {CATALOGO}.silver.ventas WHERE precio_unitario IS NULL
""").first()["filas"]
total = spark.table(f"{CATALOGO}.silver.ventas").count()
pct_precio_nulo = round(100.0 * precio_nulo / total, 2) if total else 0

resultados.append((
    "precio_unitario_castea_bien", "% de filas donde el cast de precio_unitario dio NULL",
    "WARN" if pct_precio_nulo < 1 else "FAIL", precio_nulo, pct_precio_nulo < 1,
))

# COMMAND ----------

# MAGIC %md ### Guardar en `ops.log_calidad` y aplicar el gate

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

filas_log = [
    Row(run_id=RUN_ID, capa="silver", chequeo=c, descripcion=d, severidad=s, filas_afectadas=int(n), ok=bool(ok))
    for c, d, s, n, ok in resultados
]
spark.createDataFrame(filas_log).withColumn("ejecutado_en", current_timestamp()).write.mode("append").saveAsTable(
    f"{CATALOGO}.ops.log_calidad"
)

for c, d, s, n, ok in resultados:
    print(f"[{'OK' if ok else s}] {c}: {d} (filas afectadas: {n})")

fallas = [c for c, d, s, n, ok in resultados if s == "FAIL" and not ok]
if fallas:
    raise Exception(f"Calidad de silver FALLÓ en: {fallas}. Revisar antes de seguir a gold.")

print("Calidad de silver OK, se puede seguir a gold.")
