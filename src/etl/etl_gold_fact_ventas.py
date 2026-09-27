# Databricks notebook source
# MAGIC %md
# MAGIC # ETL · gold.fact_ventas
# MAGIC
# MAGIC Se corre **después** de las 5 dimensiones (orden Kimball) — si se corriera
# MAGIC antes, las FKs no tendrían contra qué resolverse.
# MAGIC
# MAGIC El join a `dim_producto` y `dim_empleado` (las dos SCD Tipo 2) es **por
# MAGIC vigencia** (`fecha_hora BETWEEN valid_from AND valid_to`), no por "la
# MAGIC versión actual" — así una venta vieja siempre queda apuntando al precio o
# MAGIC a la sucursal que regían en ese momento, no a los de hoy.
# MAGIC
# MAGIC Regla de negocio: `cantidad <= 0` con `estado_venta = 'aprobada'` se
# MAGIC excluye acá (dato sucio, no una venta real) — silver la conserva, gold no.
# MAGIC La cantidad excluida queda registrada en `ops.log_calidad` para que sea
# MAGIC auditable por qué el conteo de la fact es menor al de silver.

# COMMAND ----------

dbutils.widgets.text("run_id", "manual", "ID de la corrida")
RUN_ID = dbutils.widgets.get("run_id") or "manual"
CATALOGO = "kiosco_la_esquina"

# COMMAND ----------

excluidas = spark.sql(f"""
    SELECT COUNT(*) AS filas FROM {CATALOGO}.silver.ventas
    WHERE cantidad <= 0 AND estado_venta = 'aprobada'
""").first()["filas"]
print(f"Filas excluidas de la fact (cantidad<=0 y aprobada): {excluidas}")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW fact_ventas_stage AS
    SELECT
      s.row_hash,
      s.ticket_id,
      CAST(date_format(s.fecha_hora, 'yyyyMMdd') AS INT)                            AS tiempo_sk,
      md5(s.sucursal_nombre)                                                        AS sucursal_sk,
      p.producto_sk,
      COALESCE(e.empleado_sk, '-1')                                                 AS empleado_sk,
      md5(concat_ws('|', COALESCE(s.metodo_pago, 'no_informado'), s.estado_venta))  AS transaccion_sk,
      s.cantidad,
      s.precio_unitario,
      s.descuento,
      CAST(s.cantidad * s.precio_unitario AS DECIMAL(12, 2))                        AS venta_neta
    FROM {CATALOGO}.silver.ventas s
    LEFT JOIN {CATALOGO}.gold.dim_producto p
      ON s.producto_id = p.producto_id AND s.fecha_hora BETWEEN p.valid_from AND p.valid_to
    LEFT JOIN {CATALOGO}.gold.dim_empleado e
      ON s.vendedor_id = e.vendedor_id AND s.fecha_hora BETWEEN e.valid_from AND e.valid_to
    WHERE NOT (s.cantidad <= 0 AND s.estado_venta = 'aprobada')
""")

sin_producto = spark.sql("SELECT COUNT(*) AS n FROM fact_ventas_stage WHERE producto_sk IS NULL").first()["n"]
if sin_producto > 0:
    raise Exception(
        f"{sin_producto} filas no encontraron versión vigente en dim_producto — "
        f"revisar que dim_producto se haya cargado antes que la fact."
    )

# COMMAND ----------

resultado = spark.sql(f"""
    MERGE INTO {CATALOGO}.gold.fact_ventas AS destino
    USING fact_ventas_stage AS origen
    ON destino.row_hash = origen.row_hash
    WHEN NOT MATCHED THEN INSERT (
      row_hash, ticket_id, tiempo_sk, sucursal_sk, producto_sk, empleado_sk,
      transaccion_sk, cantidad, precio_unitario, descuento, venta_neta, _gold_loaded_at
    ) VALUES (
      origen.row_hash, origen.ticket_id, origen.tiempo_sk, origen.sucursal_sk, origen.producto_sk,
      origen.empleado_sk, origen.transaccion_sk, origen.cantidad, origen.precio_unitario,
      origen.descuento, origen.venta_neta, current_timestamp()
    )
""")
resultado.display()

# COMMAND ----------

# MAGIC %md ### Registrar la exclusión en el log de calidad, para que sea auditable

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

spark.createDataFrame([Row(
    run_id=RUN_ID, capa="gold", chequeo="exclusion_cantidad_cero_aprobada",
    descripcion="Filas de cantidad<=0 y estado=aprobada excluidas de fact_ventas (se conservan en silver)",
    severidad="WARN", filas_afectadas=int(excluidas), ok=True,
)]).withColumn("ejecutado_en", current_timestamp()).write.mode("append").saveAsTable(f"{CATALOGO}.ops.log_calidad")

print(f"fact_ventas: {spark.table(f'{CATALOGO}.gold.fact_ventas').count()} filas totales")
