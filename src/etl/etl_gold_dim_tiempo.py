# Databricks notebook source
# MAGIC %md
# MAGIC # ETL · gold.dim_tiempo (SCD Tipo 0)
# MAGIC
# MAGIC Generada de forma combinatoria, no a partir de los datos de la fuente.
# MAGIC `INSERT OVERWRITE`: acá sí es seguro y correcto, porque cada corrida
# MAGIC regenera la tabla **completa** desde una fórmula, no una porción — es lo
# MAGIC opuesto al caso de bronze, donde `OVERWRITE` borraría historia que no
# MAGIC viene en la corrida actual. No hace falta correr esto seguido: es
# MAGIC estática, solo hay que volver a correrla si se quiere estirar el rango.

# COMMAND ----------

CATALOGO = "kiosco_la_esquina"
DESDE = "2026-09-01"   # apertura del negocio
HASTA = "2031-08-31"   # 5 años de margen hacia adelante

# COMMAND ----------

df_tiempo = spark.sql(f"""
    SELECT
      CAST(date_format(fecha, 'yyyyMMdd') AS INT)                    AS tiempo_sk,
      fecha,
      year(fecha)                                                    AS anio,
      quarter(fecha)                                                 AS trimestre,
      month(fecha)                                                   AS mes,
      CASE month(fecha)
        WHEN 1 THEN 'Enero' WHEN 2 THEN 'Febrero' WHEN 3 THEN 'Marzo'
        WHEN 4 THEN 'Abril' WHEN 5 THEN 'Mayo' WHEN 6 THEN 'Junio'
        WHEN 7 THEN 'Julio' WHEN 8 THEN 'Agosto' WHEN 9 THEN 'Septiembre'
        WHEN 10 THEN 'Octubre' WHEN 11 THEN 'Noviembre' WHEN 12 THEN 'Diciembre'
      END                                                             AS mes_nombre,
      day(fecha)                                                      AS dia,
      ((dayofweek(fecha) + 5) % 7) + 1                                AS dia_semana,
      CASE dayofweek(fecha)
        WHEN 1 THEN 'Domingo' WHEN 2 THEN 'Lunes' WHEN 3 THEN 'Martes'
        WHEN 4 THEN 'Miércoles' WHEN 5 THEN 'Jueves' WHEN 6 THEN 'Viernes'
        WHEN 7 THEN 'Sábado'
      END                                                              AS dia_semana_nombre,
      dayofweek(fecha) IN (1, 7)                                       AS es_fin_de_semana
    FROM (
      SELECT explode(sequence(to_date('{DESDE}'), to_date('{HASTA}'), interval 1 day)) AS fecha
    )
""")

df_tiempo.write.mode("overwrite").saveAsTable(f"{CATALOGO}.gold.dim_tiempo")
print(f"dim_tiempo regenerada: {df_tiempo.count()} días, de {DESDE} a {HASTA}")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {CATALOGO}.gold.dim_tiempo ORDER BY fecha LIMIT 10"))
