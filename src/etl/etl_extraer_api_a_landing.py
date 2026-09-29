# Databricks notebook source
# MAGIC %md
# MAGIC # ETL · Extraer de la API hacia landing
# MAGIC
# MAGIC **Este es uno de los dos únicos notebooks en Python del proyecto.** El
# MAGIC resto es SQL puro. Acá no hay forma de evitarlo: hay que hacer un pedido
# MAGIC HTTP a la API de ventas, y SQL no puede llamar a una API externa. El
# MAGIC Python se limita a eso: pedir los datos y guardar el JSON tal cual llega.
# MAGIC No transforma nada ni usa Spark.
# MAGIC
# MAGIC **Cómo decide qué pedir.** Mira qué rangos de fechas ya están en el
# MAGIC volumen de landing, leyendo los nombres de archivo
# MAGIC (`ventas_<desde>_<hasta>.json`):
# MAGIC - Si el volumen está vacío, es la primera vez: pide todo desde la apertura (backfill).
# MAGIC - Si ya hay archivos, pide desde el día siguiente al último `hasta` (incremental).
# MAGIC - Nunca pide el día de hoy, que todavía tiene datos parciales: el techo es ayer.
# MAGIC - Parte el rango en ventanas de 31 días como máximo, el límite de la API.
# MAGIC
# MAGIC El nombre del archivo sale de las fechas pedidas, no de la hora de la
# MAGIC corrida. Pedir el mismo rango dos veces sobreescribe el mismo archivo con el
# MAGIC mismo contenido (la API es determinista), no acumula copias.
# MAGIC
# MAGIC Si un archivo llegó a landing pero la carga a bronze falló, no se pierde:
# MAGIC `etl_landing_a_bronze.sql` carga cualquier archivo que todavía no esté en
# MAGIC bronze, sea de esta corrida o de una anterior.

# COMMAND ----------

import re
from datetime import date, timedelta

import requests

API_BASE = "https://api-sales-gamma.vercel.app"
APERTURA = date(2026, 9, 1)
TOPE_DIAS_API = 31
VOLUMEN = "/Volumes/kiosco_la_esquina/landing/raw_ventas"

# La API key es un secreto de Unity Catalog: kiosco_la_esquina.landing.api_key_ventas.
# Se crea a mano desde Catalog (ver README). Databricks la muestra como [REDACTED].
api_key = dbutils.secrets.get(catalog="kiosco_la_esquina", schema="landing", key="api_key_ventas")

# COMMAND ----------

# MAGIC %md ### ¿Desde qué fecha hay que pedir?

# COMMAND ----------

def ultimo_hasta_en_landing():
    """Devuelve el `hasta` más reciente entre los archivos de landing, o None si está vacío."""
    fechas = []
    for archivo in dbutils.fs.ls(VOLUMEN):
        encontrado = re.search(r"_(\d{4}-\d{2}-\d{2})\.json$", archivo.name)
        if encontrado:
            fechas.append(date.fromisoformat(encontrado.group(1)))
    return max(fechas) if fechas else None


def trocear(desde, hasta, max_dias=TOPE_DIAS_API):
    """Parte un rango en ventanas de a lo sumo max_dias, el límite de la API."""
    ventanas = []
    inicio = desde
    while inicio <= hasta:
        fin = min(inicio + timedelta(days=max_dias - 1), hasta)
        ventanas.append((inicio, fin))
        inicio = fin + timedelta(days=1)
    return ventanas


ultimo = ultimo_hasta_en_landing()
desde = APERTURA if ultimo is None else ultimo + timedelta(days=1)
hasta = date.today() - timedelta(days=1)

print(f"Último día ya en landing: {ultimo or 'ninguno (primera corrida, backfill)'}")

if desde > hasta:
    print("No hay días nuevos que pedir.")
    dbutils.notebook.exit("sin_datos_nuevos")

ventanas = trocear(desde, hasta)
print(f"A pedir: {desde} -> {hasta}, en {len(ventanas)} ventana(s)")

# COMMAND ----------

# MAGIC %md ### Pedir a la API y guardar el JSON crudo

# COMMAND ----------

for v_desde, v_hasta in ventanas:
    resp = requests.get(
        f"{API_BASE}/ventas",
        params={"desde": v_desde.isoformat(), "hasta": v_hasta.isoformat()},
        headers={"X-API-Key": api_key},
        timeout=60,
    )
    resp.raise_for_status()  # si la API devuelve error, la tarea del Job falla acá

    ruta = f"{VOLUMEN}/ventas_{v_desde.isoformat()}_{v_hasta.isoformat()}.json"
    dbutils.fs.put(ruta, resp.text, overwrite=True)
    print(f"  {v_desde} -> {v_hasta}: {resp.json()['meta']['filas']} filas -> {ruta}")

# COMMAND ----------

dbutils.notebook.exit(f"ok: {len(ventanas)} archivo(s) escritos, hasta {hasta.isoformat()}")
