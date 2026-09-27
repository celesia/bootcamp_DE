-- Databricks notebook source
-- MAGIC %md
-- MAGIC # ETL · gold.dim_tiempo (SCD Tipo 0)
-- MAGIC
-- MAGIC Generada de forma combinatoria con `sequence()`, no a partir de los datos.
-- MAGIC `INSERT OVERWRITE` acá sí es seguro y correcto: cada corrida regenera la
-- MAGIC tabla **completa** desde una fórmula, no una porción — lo opuesto a
-- MAGIC bronze, donde `OVERWRITE` borraría la historia que no viene en la corrida.
-- MAGIC
-- MAGIC Rango: desde la apertura del negocio (2026-09-01) hasta 5 años adelante.

-- COMMAND ----------

INSERT OVERWRITE kiosco_la_esquina.gold.dim_tiempo
SELECT
  CAST(date_format(fecha, 'yyyyMMdd') AS INT)      AS tiempo_sk,
  fecha,
  year(fecha)                                      AS anio,
  quarter(fecha)                                   AS trimestre,
  month(fecha)                                     AS mes,
  CASE month(fecha)
    WHEN 1 THEN 'Enero'      WHEN 2 THEN 'Febrero'  WHEN 3 THEN 'Marzo'
    WHEN 4 THEN 'Abril'      WHEN 5 THEN 'Mayo'     WHEN 6 THEN 'Junio'
    WHEN 7 THEN 'Julio'      WHEN 8 THEN 'Agosto'   WHEN 9 THEN 'Septiembre'
    WHEN 10 THEN 'Octubre'   WHEN 11 THEN 'Noviembre' WHEN 12 THEN 'Diciembre'
  END                                              AS mes_nombre,
  day(fecha)                                       AS dia,
  -- dayofweek() devuelve 1=domingo ... 7=sábado; esto lo pasa a ISO (1=lunes ... 7=domingo)
  ((dayofweek(fecha) + 5) % 7) + 1                 AS dia_semana,
  CASE dayofweek(fecha)
    WHEN 1 THEN 'Domingo'   WHEN 2 THEN 'Lunes'    WHEN 3 THEN 'Martes'
    WHEN 4 THEN 'Miércoles' WHEN 5 THEN 'Jueves'   WHEN 6 THEN 'Viernes'
    WHEN 7 THEN 'Sábado'
  END                                              AS dia_semana_nombre,
  dayofweek(fecha) IN (1, 7)                       AS es_fin_de_semana
FROM (
  SELECT explode(sequence(DATE'2026-09-01', DATE'2031-08-31', INTERVAL 1 DAY)) AS fecha
);

-- COMMAND ----------

SELECT COUNT(*) AS dias, MIN(fecha) AS desde, MAX(fecha) AS hasta
FROM kiosco_la_esquina.gold.dim_tiempo;

-- COMMAND ----------

SELECT * FROM kiosco_la_esquina.gold.dim_tiempo ORDER BY fecha LIMIT 10;
