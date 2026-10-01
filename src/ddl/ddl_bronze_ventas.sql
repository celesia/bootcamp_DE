-- Databricks notebook source
CREATE TABLE IF NOT EXISTS kiosco_la_esquina.bronze.ventas (
  ticket_id             STRING,
  fecha_hora            STRING,
  sucursal_nombre       STRING,
  sucursal_ciudad       STRING,
  sucursal_departamento STRING,
  vendedor_id           STRING,
  vendedor_nombre       STRING,
  producto_id           STRING,
  producto_nombre       STRING,
  categoria             STRING,
  precio_lista          STRING,
  precio_unitario       STRING,
  cantidad              STRING,
  descuento             STRING,
  metodo_pago           STRING,
  estado_venta          STRING,
  _source_file          STRING    COMMENT 'Ruta del JSON en landing del que salió esta fila',
  _ingest_timestamp     TIMESTAMP COMMENT 'Momento en que esta fila entró a bronze (UTC)',
  _run_id               STRING    COMMENT 'Identificador de la corrida que la cargó'
)
USING DELTA
COMMENT 'Ventas crudas de la API, todo en STRING, inmutable. Fuente: /ventas de Kiosco La Esquina';

-- COMMAND ----------

DESCRIBE TABLE kiosco_la_esquina.bronze.ventas;
