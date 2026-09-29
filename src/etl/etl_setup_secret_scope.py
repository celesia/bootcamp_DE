# Databricks notebook source
# MAGIC %md
# MAGIC # Setup · Secret scope para la API key
# MAGIC
# MAGIC **Se corre una sola vez, a mano. Nunca se agenda en el Job.**
# MAGIC
# MAGIC Guarda la API key de `api_sales` en un secret scope de Databricks, para
# MAGIC que el notebook de extracción la lea sin que la key aparezca en el código.
# MAGIC
# MAGIC Un secret scope no se puede crear desde la interfaz de Databricks, solo
# MAGIC por CLI o por API. Este notebook usa el SDK oficial de Databricks para
# MAGIC Python, que ya viene instalado y se autentica solo con tu usuario: no hay
# MAGIC que instalar nada ni generar tokens.
# MAGIC
# MAGIC **Cómo usarlo:**
# MAGIC 1. Correr la primera celda para que aparezcan los widgets arriba.
# MAGIC 2. Pegar la API key en el widget `api_key`.
# MAGIC 3. Correr el resto. El valor queda solo en el widget de tu sesión, nunca en el archivo ni en el repo.

# COMMAND ----------

dbutils.widgets.text("api_key", "", "API key de api_sales")

NOMBRE_SCOPE = "kiosco_secrets"
API_KEY_VALOR = dbutils.widgets.get("api_key").strip()

if not API_KEY_VALOR:
    raise ValueError("Pegá la API key en el widget 'api_key' de arriba y volvé a correr esta celda.")

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()  # dentro de un notebook se autentica solo, con tu usuario

# Crear el scope, salvo que ya exista (así se puede volver a correr sin error)
scopes_existentes = [s.name for s in w.secrets.list_scopes()]
if NOMBRE_SCOPE in scopes_existentes:
    print(f"El scope '{NOMBRE_SCOPE}' ya existía")
else:
    w.secrets.create_scope(scope=NOMBRE_SCOPE)
    print(f"Scope '{NOMBRE_SCOPE}' creado")

# Guardar la key. Si ya había una, la reemplaza.
w.secrets.put_secret(scope=NOMBRE_SCOPE, key="api_key", string_value=API_KEY_VALOR)
print("API key guardada en el scope")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación
# MAGIC Tiene que listar la clave `api_key`. Nunca muestra el valor: Databricks
# MAGIC no lo vuelve a mostrar, ni siquiera acá.

# COMMAND ----------

print(dbutils.secrets.list(NOMBRE_SCOPE))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prueba real contra la API
# MAGIC Lee la key desde el scope, igual que la va a leer la extracción. Tiene que
# MAGIC devolver 200 y la cantidad de filas del día, no un 401.

# COMMAND ----------

import requests

api_key = dbutils.secrets.get(scope=NOMBRE_SCOPE, key="api_key")
r = requests.get(
    "https://api-sales-gamma.vercel.app/ventas",
    params={"fecha": "2026-09-15"},
    headers={"X-API-Key": api_key},
    timeout=30,
)
print(r.status_code, r.json().get("meta", r.text))
