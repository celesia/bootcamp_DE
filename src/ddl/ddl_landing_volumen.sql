-- Databricks notebook source
CREATE VOLUME IF NOT EXISTS kiosco_la_esquina.landing.raw_ventas
COMMENT 'JSON crudo de /ventas, un archivo por rango de fechas distinto pedido';

-- COMMAND ----------

LIST '/Volumes/kiosco_la_esquina/landing/raw_ventas/';
