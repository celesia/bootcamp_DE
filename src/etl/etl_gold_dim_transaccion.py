# Databricks notebook source
# MAGIC %md
# MAGIC # ETL · gold.dim_transaccion (junk dimension)
# MAGIC
# MAGIC Solo crece: `MERGE ... WHEN NOT MATCHED THEN INSERT`, nunca actualiza ni
# MAGIC borra una combinación existente. Un `metodo_pago` nulo en la fuente se
# MAGIC resuelve acá como la combinación `'no_informado'`.

# COMMAND ----------

CATALOGO = "kiosco_la_esquina"

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW dim_transaccion_stage AS
    SELECT DISTINCT
      md5(concat_ws('|', COALESCE(metodo_pago, 'no_informado'), estado_venta)) AS transaccion_sk,
      COALESCE(metodo_pago, 'no_informado') AS metodo_pago,
      estado_venta
    FROM {CATALOGO}.silver.ventas
""")

# COMMAND ----------

spark.sql(f"""
    MERGE INTO {CATALOGO}.gold.dim_transaccion AS destino
    USING dim_transaccion_stage AS origen
    ON destino.transaccion_sk = origen.transaccion_sk
    WHEN NOT MATCHED THEN INSERT (transaccion_sk, metodo_pago, estado_venta, _created_at)
      VALUES (origen.transaccion_sk, origen.metodo_pago, origen.estado_venta, current_timestamp())
""")

display(spark.table(f"{CATALOGO}.gold.dim_transaccion"))
