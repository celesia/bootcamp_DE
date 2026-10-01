-- Databricks notebook source
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

-- COMMAND ----------

SHOW SCHEMAS IN kiosco_la_esquina;
