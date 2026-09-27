# Databricks notebook source
# MAGIC %md
# MAGIC # ETL · Extraer de la API hacia landing
# MAGIC
# MAGIC Primera tarea del Job. Decide sola si hace falta un backfill (primera vez,
# MAGIC sin watermark) o una carga incremental (ya hay watermark), trocea el rango
# MAGIC en ventanas de ≤31 días (límite de la API) y escribe un archivo JSON por
# MAGIC ventana en el volumen de landing.
# MAGIC
# MAGIC El nombre del archivo sale de las fechas pedidas, no de la hora de la
# MAGIC corrida — pedir el mismo rango dos veces sobreescribe el mismo archivo con
# MAGIC el mismo contenido (la API es determinista), no acumula copias.
# MAGIC
# MAGIC **No actualiza el watermark acá** — eso pasa recién en `etl_landing_a_bronze.py`,
# MAGIC después de confirmar que la carga a bronze funcionó.

# COMMAND ----------

dbutils.widgets.text("run_id", "manual", "ID de la corrida (el Job le pasa {{job.run_id}})")
RUN_ID = dbutils.widgets.get("run_id") or "manual"

CATALOGO = "kiosco_la_esquina"
API_BASE = "https://api-sales-gamma.vercel.app"
APERTURA = "2026-09-01"
TOPE_DIAS_API = 31
VOLUMEN = f"/Volumes/{CATALOGO}/landing/raw_ventas"

# COMMAND ----------

import requests
from datetime import date, timedelta

api_key = dbutils.secrets.get(scope="kiosco_secrets", key="api_key")

# COMMAND ----------

def rango_a_cargar():
    """Devuelve (desde, hasta) según el watermark, o None si no hay nada nuevo."""
    fila = spark.sql(f"""
        SELECT fecha_hasta_cargada
        FROM {CATALOGO}.ops.watermark_ingesta
        WHERE fuente = 'api_ventas'
    """).first()

    hoy = date.today()
    techo = hoy - timedelta(days=1)  # nunca pedir el día en curso (datos parciales)

    if fila is None:
        desde = date.fromisoformat(APERTURA)
    else:
        desde = fila["fecha_hasta_cargada"] + timedelta(days=1)

    if desde > techo:
        return None  # ya está todo cargado, no hay nada nuevo
    return desde, techo


def trocear(desde: date, hasta: date, max_dias: int = TOPE_DIAS_API):
    """Parte un rango en ventanas de a lo sumo max_dias, respetando el límite de la API."""
    ventanas = []
    inicio = desde
    while inicio <= hasta:
        fin = min(inicio + timedelta(days=max_dias - 1), hasta)
        ventanas.append((inicio, fin))
        inicio = fin + timedelta(days=1)
    return ventanas

# COMMAND ----------

rango = rango_a_cargar()

if rango is None:
    print("No hay datos nuevos que pedir: el watermark ya está al día.")
    dbutils.notebook.exit("sin_datos_nuevos")

desde, hasta = rango
ventanas = trocear(desde, hasta)
print(f"Rango total a pedir: {desde} -> {hasta}, en {len(ventanas)} ventana(s)")

# COMMAND ----------

archivos_escritos = []

for v_desde, v_hasta in ventanas:
    resp = requests.get(
        f"{API_BASE}/ventas",
        params={"desde": v_desde.isoformat(), "hasta": v_hasta.isoformat()},
        headers={"X-API-Key": api_key},
        timeout=60,
    )
    resp.raise_for_status()  # si la API devuelve error, cortar acá y que falle la tarea del Job

    nombre_archivo = f"ventas_{v_desde.isoformat()}_{v_hasta.isoformat()}.json"
    ruta = f"{VOLUMEN}/{nombre_archivo}"

    dbutils.fs.put(ruta, resp.text, overwrite=True)  # overwrite=True: mismo rango -> mismo archivo
    archivos_escritos.append(ruta)
    print(f"  {v_desde} -> {v_hasta}: {resp.json()['meta']['filas']} filas -> {ruta}")

print(f"\n{len(archivos_escritos)} archivo(s) escritos en landing")

# COMMAND ----------

# MAGIC %md
# MAGIC No hace falta avisarle al siguiente notebook qué archivos son nuevos:
# MAGIC `etl_landing_a_bronze.py` decide eso solo, comparando el volumen contra lo
# MAGIC que ya hay en bronze — así cada notebook se puede correr de forma
# MAGIC independiente, sin depender de que el anterior haya corrido en la misma
# MAGIC sesión.

# COMMAND ----------

dbutils.notebook.exit(f"ok: {len(archivos_escritos)} archivo(s) escritos, hasta {hasta.isoformat()}")
