-- Databricks notebook source
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

SELECT * FROM kiosco_la_esquina.gold.dim_sucursal ORDER BY sucursal_nombre;
