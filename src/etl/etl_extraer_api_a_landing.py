# Databricks notebook source
import re
from datetime import date, timedelta

import requests

API_BASE = "https://api-sales-gamma.vercel.app"
APERTURA = date(2026, 9, 1)
TOPE_DIAS_API = 31
VOLUMEN = "/Volumes/kiosco_la_esquina/landing/raw_ventas"

api_key = dbutils.secrets.get(catalog="kiosco_la_esquina", schema="landing", key="api_key_ventas")

# COMMAND ----------

def ultimo_hasta_en_landing():
    fechas = []
    for archivo in dbutils.fs.ls(VOLUMEN):
        encontrado = re.search(r"_(\d{4}-\d{2}-\d{2})\.json$", archivo.name)
        if encontrado:
            fechas.append(date.fromisoformat(encontrado.group(1)))
    return max(fechas) if fechas else None


def trocear(desde, hasta, max_dias=TOPE_DIAS_API):
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

for v_desde, v_hasta in ventanas:
    resp = requests.get(
        f"{API_BASE}/ventas",
        params={"desde": v_desde.isoformat(), "hasta": v_hasta.isoformat()},
        headers={"X-API-Key": api_key},
        timeout=60,
    )
    resp.raise_for_status()

    ruta = f"{VOLUMEN}/ventas_{v_desde.isoformat()}_{v_hasta.isoformat()}.json"
    dbutils.fs.put(ruta, resp.text, overwrite=True)
    print(f"  {v_desde} -> {v_hasta}: {resp.json()['meta']['filas']} filas -> {ruta}")

# COMMAND ----------

dbutils.notebook.exit(f"ok: {len(ventanas)} archivo(s) escritos, hasta {hasta.isoformat()}")
