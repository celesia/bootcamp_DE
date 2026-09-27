-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DDL · Silver
-- MAGIC
-- MAGIC Datos limpios, tipados y deduplicados — la fuente de verdad del proyecto.
-- MAGIC Se carga con `MERGE` (ver `etl_bronze_a_silver.sql`), nunca con `INSERT` puro.
-- MAGIC
-- MAGIC `row_hash` es la clave de deduplicación: MD5 de
-- MAGIC `ticket_id + producto_id + fecha_hora + cantidad + precio_unitario + metodo_pago`.
-- MAGIC No alcanza con `(ticket_id, producto_id)` solos, porque un mismo producto
-- MAGIC puede aparecer dos veces en el mismo ticket como líneas legítimas distintas
-- MAGIC (con otra cantidad o otro descuento) — el hash completo separa eso de un
-- MAGIC duplicado real de ingesta, que sí es idéntico en todo.
-- MAGIC
-- MAGIC Importante: silver conserva **todas** las filas válidas, incluidas las de
-- MAGIC `cantidad = 0` con `estado_venta = 'aprobada'` (dato sucio, pero real) — la
-- MAGIC decisión de excluirlas de las métricas de negocio se toma recién en gold,
-- MAGIC nunca acá. Silver es la fuente de verdad completa, no el negocio.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS kiosco_la_esquina.silver.ventas (
  row_hash                 STRING        COMMENT 'MD5 de las business keys — clave de deduplicación y de MERGE',
  ticket_id                STRING        COMMENT 'Dimensión degenerada, se mantiene en la fact',
  producto_id              INT,
  fecha_hora                TIMESTAMP,
  sucursal_nombre           STRING        COMMENT 'Normalizado: sin mayúsculas forzadas ni espacios de más',
  sucursal_ciudad           STRING,
  sucursal_departamento     STRING,
  vendedor_id               INT,
  vendedor_nombre           STRING,
  producto_nombre           STRING,
  categoria                 STRING,
  precio_lista              DECIMAL(10, 2),
  precio_unitario           DECIMAL(10, 2) COMMENT 'Ya casteado desde string con coma cuando hacía falta',
  cantidad                  INT,
  descuento                 DECIMAL(5, 4)  COMMENT 'Corregido si venía como porcentaje entero (10 -> 0.10)',
  metodo_pago               STRING         COMMENT 'NULL si la venta no tiene método informado',
  estado_venta              STRING         COMMENT 'aprobada | anulada | devolucion',
  _bronze_ingest_timestamp  TIMESTAMP      COMMENT 'Cuándo entró esta fila a bronze, heredado para trazabilidad',
  _silver_updated_at        TIMESTAMP      COMMENT 'Cuándo se insertó o actualizó en silver'
)
USING DELTA
COMMENT 'Ventas limpias, tipadas y deduplicadas. Fuente de verdad del proyecto';

-- COMMAND ----------

DESCRIBE TABLE kiosco_la_esquina.silver.ventas;
