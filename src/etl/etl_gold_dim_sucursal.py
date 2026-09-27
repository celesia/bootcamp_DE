# Databricks notebook source
# MAGIC %md
# MAGIC # ETL · gold.dim_sucursal (SCD Tipo 1)
# MAGIC
# MAGIC `MERGE`: si cambia algo de una sucursal ya conocida, se sobrescribe sin
# MAGIC conservar el valor anterior — no hace falta historial acá.

# COMMAND ----------

CATALOGO = "kiosco_la_esquina"

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW dim_sucursal_stage AS
    SELECT DISTINCT
      md5(sucursal_nombre) AS sucursal_sk,
      sucursal_nombre,
      sucursal_ciudad      AS ciudad,
      sucursal_departamento AS departamento
    FROM {CATALOGO}.silver.ventas
""")

# COMMAND ----------

spark.sql(f"""
    MERGE INTO {CATALOGO}.gold.dim_sucursal AS destino
    USING dim_sucursal_stage AS origen
    ON destino.sucursal_sk = origen.sucursal_sk
    WHEN MATCHED THEN UPDATE SET
      destino.ciudad = origen.ciudad,
      destino.departamento = origen.departamento,
      destino._updated_at = current_timestamp()
    WHEN NOT MATCHED THEN INSERT (sucursal_sk, sucursal_nombre, ciudad, departamento, _updated_at)
      VALUES (origen.sucursal_sk, origen.sucursal_nombre, origen.ciudad, origen.departamento, current_timestamp())
""")

display(spark.table(f"{CATALOGO}.gold.dim_sucursal"))
