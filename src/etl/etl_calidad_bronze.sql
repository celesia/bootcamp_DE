-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Calidad · Bronze
-- MAGIC
-- MAGIC Cada control inserta su resultado en `ops.log_calidad`. La última celda
-- MAGIC usa `assert_true()`: si algún control `FAIL` no pasó, la celda tira un
-- MAGIC error, la tarea del Job falla y la cadena se corta antes de llegar a silver.

-- COMMAND ----------

CREATE WIDGET TEXT run_id DEFAULT "manual";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Columnas del contrato presentes
-- MAGIC Si la API cambiara el shape del JSON, esto lo detecta antes de que se note
-- MAGIC más adelante como datos raros. Se consulta `information_schema`, el
-- MAGIC catálogo de metadata de Unity Catalog.

-- COMMAND ----------

INSERT INTO kiosco_la_esquina.ops.log_calidad
WITH esperadas AS (
  SELECT explode(array(
    'ticket_id', 'fecha_hora', 'sucursal_nombre', 'sucursal_ciudad', 'sucursal_departamento',
    'vendedor_id', 'vendedor_nombre', 'producto_id', 'producto_nombre', 'categoria',
    'precio_lista', 'precio_unitario', 'cantidad', 'descuento', 'metodo_pago', 'estado_venta'
  )) AS columna
),
reales AS (
  SELECT column_name
  FROM kiosco_la_esquina.information_schema.columns
  WHERE table_schema = 'bronze' AND table_name = 'ventas'
)
SELECT
  :run_id, 'bronze', 'columnas_contrato',
  'Todas las columnas esperadas están presentes en bronze',
  'FAIL',
  COUNT(*),
  COUNT(*) = 0,
  current_timestamp()
FROM esperadas
WHERE columna NOT IN (SELECT column_name FROM reales);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Nulls en columnas clave
-- MAGIC Solo informativo (`WARN`): bronze conserva todo tal cual llegó, incluidos los nulls.

-- COMMAND ----------

INSERT INTO kiosco_la_esquina.ops.log_calidad
SELECT
  :run_id, 'bronze', 'nulls_columnas_clave',
  'Nulls en ticket_id/fecha_hora/producto_id (informativo)',
  'WARN',
  COUNT_IF(ticket_id IS NULL OR fecha_hora IS NULL OR producto_id IS NULL),
  COUNT_IF(ticket_id IS NULL OR fecha_hora IS NULL OR producto_id IS NULL) = 0,
  current_timestamp()
FROM kiosco_la_esquina.bronze.ventas;

-- COMMAND ----------

-- MAGIC %md ### Resultado de esta corrida

-- COMMAND ----------

SELECT chequeo, severidad, filas_afectadas, ok
FROM kiosco_la_esquina.ops.log_calidad
WHERE run_id = :run_id AND capa = 'bronze'
ORDER BY ejecutado_en DESC;

-- COMMAND ----------

-- MAGIC %md ### Gate: si algún control FAIL no pasó, cortar acá

-- COMMAND ----------

SELECT assert_true(
  (SELECT COUNT(*) FROM kiosco_la_esquina.ops.log_calidad
   WHERE run_id = :run_id AND capa = 'bronze' AND severidad = 'FAIL' AND NOT ok) = 0,
  'Calidad de bronze FALLÓ. Revisar ops.log_calidad antes de seguir a silver.'
) AS gate_bronze;
