# Databricks notebook source
# MAGIC %md
# MAGIC # Calidad · Bronze
# MAGIC
# MAGIC Corre después de cargar bronze, antes de tocar silver. Si un check
# MAGIC marcado `FAIL` no pasa, la celda final tira una excepción — eso corta la
# MAGIC tarea del Job y evita que datos con un contrato roto lleguen a silver.

# COMMAND ----------

dbutils.widgets.text("run_id", "manual", "ID de la corrida")
RUN_ID = dbutils.widgets.get("run_id") or "manual"
CATALOGO = "kiosco_la_esquina"

# COMMAND ----------

resultados = []  # (chequeo, descripcion, severidad, filas_afectadas, ok)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Columnas del contrato presentes
# MAGIC Si la API cambiara el shape del JSON, esto lo detecta antes de que se
# MAGIC note más adelante como datos raros.

# COMMAND ----------

columnas_esperadas = {
    "ticket_id", "fecha_hora", "sucursal_nombre", "sucursal_ciudad", "sucursal_departamento",
    "vendedor_id", "vendedor_nombre", "producto_id", "producto_nombre", "categoria",
    "precio_lista", "precio_unitario", "cantidad", "descuento", "metodo_pago", "estado_venta",
}
columnas_reales = set(spark.table(f"{CATALOGO}.bronze.ventas").columns)
faltantes = columnas_esperadas - columnas_reales

resultados.append((
    "columnas_contrato", "Todas las columnas esperadas están presentes en bronze",
    "FAIL", len(faltantes), len(faltantes) == 0,
))
if faltantes:
    print(f"Faltan columnas: {faltantes}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Nulls en columnas clave (como string, bronze nunca filtra)
# MAGIC Solo informativo — bronze conserva todo tal cual llegó, incluidos los nulls.

# COMMAND ----------

nulls = spark.sql(f"""
    SELECT
      SUM(CASE WHEN ticket_id IS NULL THEN 1 ELSE 0 END) AS ticket_id_nulo,
      SUM(CASE WHEN fecha_hora IS NULL THEN 1 ELSE 0 END) AS fecha_hora_nula,
      SUM(CASE WHEN producto_id IS NULL THEN 1 ELSE 0 END) AS producto_id_nulo
    FROM {CATALOGO}.bronze.ventas
""").first()

total_nulls_clave = nulls["ticket_id_nulo"] + nulls["fecha_hora_nula"] + nulls["producto_id_nulo"]
resultados.append((
    "nulls_columnas_clave", "Nulls en ticket_id/fecha_hora/producto_id (informativo)",
    "WARN", total_nulls_clave, total_nulls_clave == 0,
))
print(f"Nulls en columnas clave: {dict(nulls.asDict())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Guardar el resultado en `ops.log_calidad`

# COMMAND ----------

from pyspark.sql import Row

filas_log = [
    Row(run_id=RUN_ID, capa="bronze", chequeo=c, descripcion=d, severidad=s, filas_afectadas=int(n), ok=bool(ok))
    for c, d, s, n, ok in resultados
]
spark.createDataFrame(filas_log).withColumn(
    "ejecutado_en", __import__("pyspark").sql.functions.current_timestamp()
).write.mode("append").saveAsTable(f"{CATALOGO}.ops.log_calidad")

for c, d, s, n, ok in resultados:
    print(f"[{'OK' if ok else s}] {c}: {d} (filas afectadas: {n})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gate: si algún check FAIL no pasó, cortar acá

# COMMAND ----------

fallas = [c for c, d, s, n, ok in resultados if s == "FAIL" and not ok]
if fallas:
    raise Exception(f"Calidad de bronze FALLÓ en: {fallas}. Revisar antes de seguir a silver.")

print("Calidad de bronze OK, se puede seguir a silver.")
