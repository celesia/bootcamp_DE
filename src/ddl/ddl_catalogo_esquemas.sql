-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DDL · Catálogo y esquemas
-- MAGIC
-- MAGIC Solo define estructura. No inserta ni transforma ningún dato — eso vive en `src/etl`.
-- MAGIC Usa `IF NOT EXISTS` en todo, así que correrlo de nuevo nunca rompe nada.
-- MAGIC
-- MAGIC | Esquema | Qué guarda |
-- MAGIC |---|---|
-- MAGIC | `landing` | Volumen con el JSON crudo tal cual lo devuelve la API |
-- MAGIC | `bronze` | Datos crudos en STRING, inmutables, solo se agregan filas |
-- MAGIC | `silver` | Datos limpios, tipados y deduplicados — fuente de verdad |
-- MAGIC | `gold` | Modelo dimensional (esquema estrella + junk dimension) |
-- MAGIC | `semantica` | Vistas de solo lectura para consultas de negocio |
-- MAGIC | `ops` | Watermark de la ingesta y log de calidad |

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS kiosco_la_esquina
COMMENT 'Proyecto final del bootcamp: ventas de Kiosco La Esquina (Uruguay)';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS kiosco_la_esquina.landing
COMMENT 'JSON crudo tal cual llega de la API, un archivo por rango de fechas';

CREATE SCHEMA IF NOT EXISTS kiosco_la_esquina.bronze
COMMENT 'Datos crudos en STRING, inmutables. Solo se agrega, nunca se pisa ni se limpia';

CREATE SCHEMA IF NOT EXISTS kiosco_la_esquina.silver
COMMENT 'Datos limpios, tipados y deduplicados. Fuente de verdad del proyecto';

CREATE SCHEMA IF NOT EXISTS kiosco_la_esquina.gold
COMMENT 'Modelo dimensional: dimensiones + fact, listo para consultarse sin transformar más';

CREATE SCHEMA IF NOT EXISTS kiosco_la_esquina.semantica
COMMENT 'Vistas de solo lectura sobre gold. Nunca tablas, nunca escritura';

CREATE SCHEMA IF NOT EXISTS kiosco_la_esquina.ops
COMMENT 'Control del pipeline: watermark de ingesta y log de calidad';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Verificación rápida: tienen que aparecer los 6 esquemas.

-- COMMAND ----------

SHOW SCHEMAS IN kiosco_la_esquina;
