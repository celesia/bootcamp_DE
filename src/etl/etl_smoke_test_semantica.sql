-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Smoke test · Capa semántica
-- MAGIC
-- MAGIC Última tarea del Job. Las vistas no se cargan (reflejan gold solas), así
-- MAGIC que lo único que hace falta es confirmar que responden y no vuelven
-- MAGIC vacías. Si alguna vuelve vacía, `assert_true()` corta la tarea y el Job
-- MAGIC queda marcado como fallido.

-- COMMAND ----------

SELECT 'vw_ventas_diarias_por_sucursal' AS vista, COUNT(*) AS filas FROM kiosco_la_esquina.semantica.vw_ventas_diarias_por_sucursal
UNION ALL
SELECT 'vw_top_productos', COUNT(*) FROM kiosco_la_esquina.semantica.vw_top_productos
UNION ALL
SELECT 'vw_desempeno_vendedores', COUNT(*) FROM kiosco_la_esquina.semantica.vw_desempeno_vendedores
UNION ALL
SELECT 'vw_devoluciones', COUNT(*) FROM kiosco_la_esquina.semantica.vw_devoluciones;

-- COMMAND ----------

SELECT assert_true(
  (SELECT COUNT(*) FROM kiosco_la_esquina.semantica.vw_ventas_diarias_por_sucursal) > 0
  AND (SELECT COUNT(*) FROM kiosco_la_esquina.semantica.vw_top_productos) > 0
  AND (SELECT COUNT(*) FROM kiosco_la_esquina.semantica.vw_desempeno_vendedores) > 0
  AND (SELECT COUNT(*) FROM kiosco_la_esquina.semantica.vw_devoluciones) > 0,
  'Alguna vista de la capa semántica volvió vacía.'
) AS gate_semantica;
