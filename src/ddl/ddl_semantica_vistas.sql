-- Databricks notebook source
CREATE OR REPLACE VIEW kiosco_la_esquina.semantica.vw_ventas_diarias_por_sucursal AS
SELECT
  t.fecha,
  s.sucursal_nombre,
  s.ciudad,
  s.departamento,
  COUNT(DISTINCT f.ticket_id) AS tickets,
  SUM(f.cantidad)             AS unidades,
  SUM(f.venta_neta)           AS facturacion
FROM kiosco_la_esquina.gold.fact_ventas f
JOIN kiosco_la_esquina.gold.dim_tiempo    t ON f.tiempo_sk   = t.tiempo_sk
JOIN kiosco_la_esquina.gold.dim_sucursal  s ON f.sucursal_sk = s.sucursal_sk
GROUP BY t.fecha, s.sucursal_nombre, s.ciudad, s.departamento;

-- COMMAND ----------

CREATE OR REPLACE VIEW kiosco_la_esquina.semantica.vw_top_productos AS
SELECT
  p.producto_id,
  p.producto_nombre,
  p.categoria,
  SUM(f.cantidad)   AS unidades_vendidas,
  SUM(f.venta_neta) AS facturacion,
  RANK() OVER (ORDER BY SUM(f.venta_neta) DESC) AS ranking_facturacion
FROM kiosco_la_esquina.gold.fact_ventas f
JOIN kiosco_la_esquina.gold.dim_producto p ON f.producto_sk = p.producto_sk
GROUP BY p.producto_id, p.producto_nombre, p.categoria;

-- COMMAND ----------

CREATE OR REPLACE VIEW kiosco_la_esquina.semantica.vw_desempeno_vendedores AS
SELECT
  ea.vendedor_id,
  ea.vendedor_nombre,
  ea.sucursal_nombre                                          AS sucursal_actual,
  COUNT(DISTINCT f.ticket_id)                                 AS tickets_atendidos,
  SUM(f.venta_neta)                                           AS venta_total,
  ROUND(SUM(f.venta_neta) / COUNT(DISTINCT f.ticket_id), 2)   AS ticket_promedio
FROM kiosco_la_esquina.gold.fact_ventas f
JOIN kiosco_la_esquina.gold.dim_empleado eh ON f.empleado_sk = eh.empleado_sk
JOIN kiosco_la_esquina.gold.dim_empleado ea ON eh.vendedor_id = ea.vendedor_id AND ea.is_current = true
GROUP BY ea.vendedor_id, ea.vendedor_nombre, ea.sucursal_nombre;

-- COMMAND ----------

CREATE OR REPLACE VIEW kiosco_la_esquina.semantica.vw_devoluciones AS
SELECT
  s.sucursal_nombre,
  p.producto_nombre,
  p.categoria,
  COUNT(*)           AS lineas_devueltas,
  SUM(f.venta_neta)  AS monto_devuelto
FROM kiosco_la_esquina.gold.fact_ventas f
JOIN kiosco_la_esquina.gold.dim_transaccion tr ON f.transaccion_sk = tr.transaccion_sk
JOIN kiosco_la_esquina.gold.dim_sucursal    s  ON f.sucursal_sk    = s.sucursal_sk
JOIN kiosco_la_esquina.gold.dim_producto    p  ON f.producto_sk    = p.producto_sk
WHERE tr.estado_venta = 'devolucion'
GROUP BY s.sucursal_nombre, p.producto_nombre, p.categoria;

-- COMMAND ----------

SELECT 'vw_ventas_diarias_por_sucursal' AS vista, COUNT(*) AS filas FROM kiosco_la_esquina.semantica.vw_ventas_diarias_por_sucursal
UNION ALL
SELECT 'vw_top_productos', COUNT(*) FROM kiosco_la_esquina.semantica.vw_top_productos
UNION ALL
SELECT 'vw_desempeno_vendedores', COUNT(*) FROM kiosco_la_esquina.semantica.vw_desempeno_vendedores
UNION ALL
SELECT 'vw_devoluciones', COUNT(*) FROM kiosco_la_esquina.semantica.vw_devoluciones;
