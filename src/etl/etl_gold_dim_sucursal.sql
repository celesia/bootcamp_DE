-- Databricks notebook source
-- MAGIC %md
-- MAGIC # ETL · gold.dim_sucursal (SCD Tipo 1)
-- MAGIC
-- MAGIC `MERGE`: si cambia algo de una sucursal ya conocida, se sobrescribe sin
-- MAGIC conservar el valor anterior. No hace falta historial acá.

-- COMMAND ----------

MERGE INTO kiosco_la_esquina.gold.dim_sucursal AS destino
USING (
  SELECT DISTINCT
    md5(sucursal_nombre)  AS sucursal_sk,
    sucursal_nombre,
    sucursal_ciudad       AS ciudad,
    sucursal_departamento AS departamento
  FROM kiosco_la_esquina.silver.ventas
) AS origen
ON destino.sucursal_sk = origen.sucursal_sk
WHEN MATCHED THEN UPDATE SET
  destino.ciudad       = origen.ciudad,
  destino.departamento = origen.departamento,
  destino._updated_at  = current_timestamp()
WHEN NOT MATCHED THEN INSERT (sucursal_sk, sucursal_nombre, ciudad, departamento, _updated_at)
  VALUES (origen.sucursal_sk, origen.sucursal_nombre, origen.ciudad, origen.departamento, current_timestamp());

-- COMMAND ----------

-- Tienen que ser exactamente 5. Si aparecen más, la normalización de nombres en silver no está funcionando.
SELECT * FROM kiosco_la_esquina.gold.dim_sucursal ORDER BY sucursal_nombre;
