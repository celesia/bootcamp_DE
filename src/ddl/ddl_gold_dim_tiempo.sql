-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DDL · gold.dim_tiempo (SCD Tipo 0)
-- MAGIC
-- MAGIC Generada una sola vez, de forma combinatoria — nunca se recarga ni se
-- MAGIC actualiza. `tiempo_sk` usa el formato `YYYYMMDD` como entero: es estable,
-- MAGIC legible y no hace falta calcularlo con un hash.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS kiosco_la_esquina.gold.dim_tiempo (
  tiempo_sk        INT     COMMENT 'YYYYMMDD, ej. 20260915',
  fecha             DATE,
  anio              INT,
  trimestre         INT,
  mes               INT,
  mes_nombre        STRING,
  dia               INT,
  dia_semana        INT     COMMENT '1=lunes ... 7=domingo',
  dia_semana_nombre STRING,
  es_fin_de_semana  BOOLEAN
)
USING DELTA
COMMENT 'SCD 0: estática, generada una sola vez. No se recarga';

-- COMMAND ----------

DESCRIBE TABLE kiosco_la_esquina.gold.dim_tiempo;
