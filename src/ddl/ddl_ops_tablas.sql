-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DDL · Tabla de control (`ops`)
-- MAGIC
-- MAGIC No es parte del modelo de negocio: sostiene el pipeline en sí.
-- MAGIC
-- MAGIC `log_calidad` guarda cada control de calidad ejecutado, con su resultado.
-- MAGIC Los notebooks de calidad la leen al final para decidir si cortan el Job,
-- MAGIC y queda como historia de todas las corridas.
-- MAGIC
-- MAGIC No hace falta una tabla de watermark: el pipeline sabe qué días le faltan
-- MAGIC por los nombres de archivo en landing, y qué archivos falta cargar lo
-- MAGIC resuelve `COPY INTO`, que recuerda los archivos ya cargados en bronze.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS kiosco_la_esquina.ops.log_calidad (
  run_id          STRING,
  capa            STRING    COMMENT 'bronze | silver | gold',
  chequeo         STRING    COMMENT 'Nombre corto del control',
  descripcion     STRING,
  severidad       STRING    COMMENT 'FAIL corta la cadena del Job. WARN solo queda registrado',
  filas_afectadas BIGINT,
  ok              BOOLEAN,
  ejecutado_en    TIMESTAMP
)
USING DELTA
COMMENT 'Log histórico de todos los controles de calidad, append-only';

-- COMMAND ----------

DESCRIBE TABLE kiosco_la_esquina.ops.log_calidad;
