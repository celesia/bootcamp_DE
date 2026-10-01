-- Databricks notebook source
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
