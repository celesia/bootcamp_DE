-- Databricks notebook source
-- MAGIC %md
-- MAGIC # ETL · Landing → Bronze
-- MAGIC
-- MAGIC SQL puro, una sola sentencia: `COPY INTO`, la función nativa de Delta
-- MAGIC para cargar archivos de una carpeta a una tabla.
-- MAGIC
-- MAGIC **Recuerda qué archivos ya cargó.** Delta guarda en el log de la tabla
-- MAGIC la lista de archivos procesados, y en cada corrida saltea los que ya
-- MAGIC estaban, aunque se hayan modificado después. Correr este notebook dos
-- MAGIC veces no duplica nada. Es el control a nivel de archivo.
-- MAGIC
-- MAGIC **No controla filas.** Si un archivo trae la misma venta dos veces (la API
-- MAGIC inyecta un duplicado a propósito), las dos llegan a bronze tal cual. Las
-- MAGIC limpia silver.
-- MAGIC
-- MAGIC **El punto crítico es `primitivesAsString`.** Hace que todo valor llegue
-- MAGIC como texto. Sin esto, Databricks infiere el tipo de cada campo mirando los
-- MAGIC datos, y `precio_unitario` mezcla números con textos como `"260,0"`. Con
-- MAGIC la opción, todo se guarda como STRING y el casteo recién pasa en silver.
-- MAGIC
-- MAGIC **Cómo se arma cada fila.** Cada archivo es la respuesta completa de la
-- MAGIC API, con un array `data` de ventas. `inline(data)` lo desarma en una fila
-- MAGIC por venta, con una columna por campo. `COPY INTO` empareja columnas por
-- MAGIC nombre, así que el orden de los campos en el JSON no importa.
-- MAGIC
-- MAGIC **Si hace falta recargar un archivo a propósito**, borrar sus filas de
-- MAGIC bronze no alcanza, porque Delta lo sigue recordando como cargado. Hay que
-- MAGIC agregar `'force' = 'true'` en `COPY_OPTIONS`. Como la API es determinista,
-- MAGIC en este proyecto no debería hacer falta nunca.

-- COMMAND ----------

CREATE WIDGET TEXT run_id DEFAULT "manual";

-- COMMAND ----------

COPY INTO kiosco_la_esquina.bronze.ventas
FROM (
  SELECT
    inline(data),
    _metadata.file_path  AS _source_file,
    current_timestamp()  AS _ingest_timestamp,
    :run_id              AS _run_id
  FROM '/Volumes/kiosco_la_esquina/landing/raw_ventas/'
)
FILEFORMAT = JSON
FORMAT_OPTIONS ('multiLine' = 'true', 'primitivesAsString' = 'true');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Qué cargó esta corrida
-- MAGIC Si no había archivos nuevos en landing, esto vuelve vacío y está bien.

-- COMMAND ----------

SELECT _source_file, COUNT(*) AS filas
FROM kiosco_la_esquina.bronze.ventas
WHERE _run_id = :run_id
GROUP BY _source_file;
