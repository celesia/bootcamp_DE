-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Calidad · Bronze
-- MAGIC
-- MAGIC Corre después de cargar bronze, antes de tocar silver. Cada control es un
-- MAGIC `assert_true()`: si la condición no se cumple, la celda tira un error, la
-- MAGIC tarea del Job falla y la cadena se corta antes de llegar a silver.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Columnas del contrato presentes
-- MAGIC Si la API cambiara el shape del JSON, esto lo detecta antes de que se note
-- MAGIC más adelante como datos raros. Se consulta `information_schema`, el
-- MAGIC catálogo de metadata de Unity Catalog. Tiene que volver vacía.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW columnas_faltantes AS
SELECT columna
FROM (
  SELECT explode(array(
    'ticket_id', 'fecha_hora', 'sucursal_nombre', 'sucursal_ciudad', 'sucursal_departamento',
    'vendedor_id', 'vendedor_nombre', 'producto_id', 'producto_nombre', 'categoria',
    'precio_lista', 'precio_unitario', 'cantidad', 'descuento', 'metodo_pago', 'estado_venta'
  )) AS columna
)
WHERE columna NOT IN (
  SELECT column_name
  FROM kiosco_la_esquina.information_schema.columns
  WHERE table_schema = 'bronze' AND table_name = 'ventas'
);

SELECT * FROM columnas_faltantes;

-- COMMAND ----------

SELECT assert_true(
  (SELECT COUNT(*) FROM columnas_faltantes) = 0,
  'Faltan columnas del contrato en bronze.ventas: la API cambió la forma del JSON.'
) AS control_columnas;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Nulls en columnas clave
-- MAGIC Solo informativo, no corta el Job: bronze conserva todo tal cual llegó,
-- MAGIC incluidos los nulls.

-- COMMAND ----------

SELECT
  COUNT(*)                                AS filas,
  COUNT_IF(ticket_id IS NULL)             AS ticket_id_nulo,
  COUNT_IF(fecha_hora IS NULL)            AS fecha_hora_nula,
  COUNT_IF(producto_id IS NULL)           AS producto_id_nulo
FROM kiosco_la_esquina.bronze.ventas;
