-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DDL · Bronze
-- MAGIC
-- MAGIC Todas las columnas de negocio son **STRING**, sin excepción — incluida
-- MAGIC `precio_unitario`, que en la API a veces llega como número y a veces como
-- MAGIC texto con coma decimal (`"260,0"`). Bronze guarda el dato tal cual llegó;
-- MAGIC el casteo recién pasa en silver.
-- MAGIC
-- MAGIC Se carga con `COPY INTO` (ver `etl_landing_a_bronze.sql`), que solo agrega
-- MAGIC filas y nunca modifica las que ya están. Delta recuerda qué archivos ya
-- MAGIC cargó, así que reprocesar el mismo día no duplica nada. Lo que no hace es
-- MAGIC filtrar filas repetidas dentro de un archivo: esas llegan tal cual y las
-- MAGIC limpia silver. Bronze es la fotografía cruda de todo lo que llegó.
-- MAGIC
-- MAGIC Columnas de metadata (`_` adelante) para poder auditar cada carga.

-- COMMAND ----------

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
