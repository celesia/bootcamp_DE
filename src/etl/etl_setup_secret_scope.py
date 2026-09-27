# Databricks notebook source
# MAGIC %md
# MAGIC # Setup · Secret scope para la API key
# MAGIC
# MAGIC **Se corre una sola vez, a mano. Nunca se agenda en el Job.**
# MAGIC
# MAGIC Crear un secret scope solo se puede por CLI o por API REST — no existe una
# MAGIC opción en la interfaz de Databricks, en ningún plan. Este notebook lo hace
# MAGIC por REST API directamente desde una celda, así no hace falta instalar el
# MAGIC Databricks CLI en tu máquina.
# MAGIC
# MAGIC **Si la celda de abajo falla** (algunos workspaces restringen que un
# MAGIC notebook lea su propio token, por seguridad), usá el camino alternativo:
# MAGIC 1. Instalar el CLI: `pip install databricks-cli`
# MAGIC 2. Generar un token en el workspace: perfil (arriba a la derecha) → Settings → Developer → Access tokens → Generate new token
# MAGIC 3. `databricks configure --token` (pegar la URL del workspace y el token)
# MAGIC 4. `databricks secrets create-scope kiosco_secrets`
# MAGIC 5. `databricks secrets put-secret kiosco_secrets api_key` (te va a pedir el valor)

# COMMAND ----------

dbutils.widgets.text("nombre_scope", "kiosco_secrets", "Nombre del secret scope")
dbutils.widgets.text("api_key", "", "Pegá acá la API key de api_sales (no queda en el código, solo en esta celda)")

NOMBRE_SCOPE = dbutils.widgets.get("nombre_scope")
API_KEY_VALOR = dbutils.widgets.get("api_key")

if not API_KEY_VALOR:
    raise ValueError("Completá el widget 'api_key' con el valor real antes de correr esta celda")

# COMMAND ----------

import requests

ctx = dbutils.notebook.getContext()
host = ctx.apiUrl().get()
token = ctx.apiToken().get()
headers = {"Authorization": f"Bearer {token}"}

# COMMAND ----------

# Crear el scope (si ya existe, la API devuelve error "RESOURCE_ALREADY_EXISTS" — se ignora)
resp = requests.post(f"{host}/api/2.0/secrets/scopes/create", headers=headers, json={"scope": NOMBRE_SCOPE})
if resp.status_code == 200 or "RESOURCE_ALREADY_EXISTS" in resp.text:
    print(f"Scope '{NOMBRE_SCOPE}' listo")
else:
    print(f"ATENCIÓN — revisar: {resp.status_code} {resp.text}")

# COMMAND ----------

# Guardar el valor de la API key dentro del scope
resp = requests.post(
    f"{host}/api/2.0/secrets/put",
    headers=headers,
    json={"scope": NOMBRE_SCOPE, "key": "api_key", "string_value": API_KEY_VALOR},
)
print(f"put-secret: {resp.status_code} {resp.text or 'OK'}")

# COMMAND ----------

# MAGIC %md
# MAGIC Verificación: tiene que listar el scope y la clave `api_key` (nunca el
# MAGIC valor — Databricks nunca lo vuelve a mostrar, ni siquiera acá).

# COMMAND ----------

print(dbutils.secrets.listScopes())
print(dbutils.secrets.list(NOMBRE_SCOPE))

# COMMAND ----------

# MAGIC %md
# MAGIC Prueba real: esto tiene que traer datos, no un 401.

# COMMAND ----------

import requests

api_key = dbutils.secrets.get(scope=NOMBRE_SCOPE, key="api_key")
r = requests.get(
    "https://api-sales-gamma.vercel.app/ventas",
    params={"fecha": "2026-09-15"},
    headers={"X-API-Key": api_key},
    timeout=30,
)
print(r.status_code, r.json()["meta"])
