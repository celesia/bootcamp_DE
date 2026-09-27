-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DDL · Volumen de landing
-- MAGIC
-- MAGIC Un volumen de Unity Catalog para guardar el JSON crudo que devuelve la API,
-- MAGIC antes de parsearlo hacia bronze. El nombre de cada archivo sale del rango de
-- MAGIC fechas pedido (`ventas_<desde>_<hasta>.json`), nunca de la hora de la corrida —
-- MAGIC así pedir el mismo rango dos veces sobreescribe el mismo archivo en vez de
-- MAGIC acumular copias idénticas (la API es determinista por fecha).

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS kiosco_la_esquina.landing.raw_ventas
COMMENT 'JSON crudo de /ventas, un archivo por rango de fechas distinto pedido';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Verificación: la ruta del volumen para usar desde Python es
-- MAGIC `/Volumes/kiosco_la_esquina/landing/raw_ventas/`.

-- COMMAND ----------

LIST '/Volumes/kiosco_la_esquina/landing/raw_ventas/';
