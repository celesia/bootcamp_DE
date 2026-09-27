# Databricks notebook source
# MAGIC %md
# MAGIC # Calidad · Gold
# MAGIC
# MAGIC Corre después de cargar la fact, antes del smoke test de la capa
# MAGIC semántica. Es la última red de seguridad del pipeline.

# COMMAND ----------

dbutils.widgets.text("run_id", "manual", "ID de la corrida")
RUN_ID = dbutils.widgets.get("run_id") or "manual"
CATALOGO = "kiosco_la_esquina"

# COMMAND ----------

resultados = []

# COMMAND ----------

# MAGIC %md ### Integridad referencial: 0 FKs huérfanas en la fact

# COMMAND ----------

huerfanas = spark.sql(f"""
    SELECT COUNT(*) AS n
    FROM {CATALOGO}.gold.fact_ventas f
    LEFT JOIN {CATALOGO}.gold.dim_tiempo t       ON f.tiempo_sk = t.tiempo_sk
    LEFT JOIN {CATALOGO}.gold.dim_sucursal s     ON f.sucursal_sk = s.sucursal_sk
    LEFT JOIN {CATALOGO}.gold.dim_producto p     ON f.producto_sk = p.producto_sk
    LEFT JOIN {CATALOGO}.gold.dim_empleado e     ON f.empleado_sk = e.empleado_sk
    LEFT JOIN {CATALOGO}.gold.dim_transaccion tr ON f.transaccion_sk = tr.transaccion_sk
    WHERE t.tiempo_sk IS NULL OR s.sucursal_sk IS NULL OR p.producto_sk IS NULL
       OR e.empleado_sk IS NULL OR tr.transaccion_sk IS NULL
""").first()["n"]

resultados.append(("integridad_referencial", "0 filas de fact_ventas sin match en alguna dimensión", "FAIL", huerfanas, huerfanas == 0))

# COMMAND ----------

# MAGIC %md ### Máximo 1 versión vigente por business key, en las dos SCD Tipo 2

# COMMAND ----------

multi_producto = spark.sql(f"""
    SELECT COUNT(*) AS n FROM (
      SELECT producto_id FROM {CATALOGO}.gold.dim_producto WHERE is_current = true
      GROUP BY producto_id HAVING COUNT(*) > 1
    )
""").first()["n"]
resultados.append(("un_solo_vigente_producto", "Máximo 1 is_current=true por producto_id", "FAIL", multi_producto, multi_producto == 0))

multi_empleado = spark.sql(f"""
    SELECT COUNT(*) AS n FROM (
      SELECT vendedor_id FROM {CATALOGO}.gold.dim_empleado WHERE is_current = true AND vendedor_id IS NOT NULL
      GROUP BY vendedor_id HAVING COUNT(*) > 1
    )
""").first()["n"]
resultados.append(("un_solo_vigente_empleado", "Máximo 1 is_current=true por vendedor_id", "FAIL", multi_empleado, multi_empleado == 0))

# COMMAND ----------

# MAGIC %md ### Sin `row_hash` duplicados en la fact

# COMMAND ----------

dup_fact = spark.sql(f"""
    SELECT COUNT(*) AS n FROM (
      SELECT row_hash FROM {CATALOGO}.gold.fact_ventas GROUP BY row_hash HAVING COUNT(*) > 1
    )
""").first()["n"]
resultados.append(("row_hash_unico_fact", "row_hash no se repite en fact_ventas", "FAIL", dup_fact, dup_fact == 0))

# COMMAND ----------

# MAGIC %md ### Reconciliación: fact vs. silver (la diferencia debe ser exactamente lo excluido)

# COMMAND ----------

total_silver = spark.table(f"{CATALOGO}.silver.ventas").count()
total_fact = spark.table(f"{CATALOGO}.gold.fact_ventas").count()
excluidas_esperadas = spark.sql(f"""
    SELECT COUNT(*) AS n FROM {CATALOGO}.silver.ventas WHERE cantidad <= 0 AND estado_venta = 'aprobada'
""").first()["n"]

diferencia_inesperada = (total_silver - total_fact) - excluidas_esperadas
resultados.append((
    "reconciliacion_fact_silver",
    f"silver({total_silver}) - fact({total_fact}) debe ser igual a las filas excluidas ({excluidas_esperadas})",
    "FAIL", abs(diferencia_inesperada), diferencia_inesperada == 0,
))

# COMMAND ----------

# MAGIC %md ### Guardar y aplicar el gate

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

filas_log = [
    Row(run_id=RUN_ID, capa="gold", chequeo=c, descripcion=d, severidad=s, filas_afectadas=int(n), ok=bool(ok))
    for c, d, s, n, ok in resultados
]
spark.createDataFrame(filas_log).withColumn("ejecutado_en", current_timestamp()).write.mode("append").saveAsTable(
    f"{CATALOGO}.ops.log_calidad"
)

for c, d, s, n, ok in resultados:
    print(f"[{'OK' if ok else s}] {c}: {d} (filas afectadas: {n})")

fallas = [c for c, d, s, n, ok in resultados if s == "FAIL" and not ok]
if fallas:
    raise Exception(f"Calidad de gold FALLÓ en: {fallas}.")

print("Calidad de gold OK. Pipeline completo.")
