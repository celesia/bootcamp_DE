-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DDL · Tablas de control (`ops`)
-- MAGIC
-- MAGIC No son parte del modelo de negocio: sostienen el pipeline en sí.
-- MAGIC
-- MAGIC | Tabla | Para qué |
-- MAGIC |---|---|
-- MAGIC | `watermark_ingesta` | Hasta qué fecha está confirmada en bronze. Registro de auditoría de cada carga |
-- MAGIC | `log_calidad` | Historial de cada control de calidad ejecutado, con su resultado |

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS kiosco_la_esquina.ops.watermark_ingesta (
  fuente              STRING    COMMENT "Nombre de la fuente, ej. 'api_ventas'",
  fecha_hasta_cargada DATE      COMMENT 'Último día confirmado en bronze',
  run_id              STRING,
  actualizado_en      TIMESTAMP
)
USING DELTA
COMMENT 'Una fila por fuente. Se actualiza recién después de confirmar la carga a bronze';

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

DESCRIBE TABLE kiosco_la_esquina.ops.watermark_ingesta;
DESCRIBE TABLE kiosco_la_esquina.ops.log_calidad;
