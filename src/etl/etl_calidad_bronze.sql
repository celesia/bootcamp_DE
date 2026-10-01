-- Databricks notebook source
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

SELECT
  COUNT(*)                                AS filas,
  COUNT_IF(ticket_id IS NULL)             AS ticket_id_nulo,
  COUNT_IF(fecha_hora IS NULL)            AS fecha_hora_nula,
  COUNT_IF(producto_id IS NULL)           AS producto_id_nulo
FROM kiosco_la_esquina.bronze.ventas;
