# Kiosco La Esquina — Pipeline de Datos

Proyecto final del bootcamp de ingeniería de datos. Pipeline completo Bronze → Silver → Gold en Databricks, con MERGE, SCD Tipo 2, validaciones de calidad y workflow automatizado, sobre datos de ventas de una cadena ficticia de kioscos en Uruguay.

**[Tu nombre]** · [tu LinkedIn]

## Qué hace

Ingiere las ventas diarias desde una API propia ([`api_sales`](https://github.com/celesia/api_sales), desplegada en Vercel), las limpia y las modela en un esquema estrella listo para responder preguntas de negocio: qué sucursal vende más, qué productos rankean mejor, cómo rinde cada vendedor, y dónde se concentran las devoluciones.

## Arquitectura

```
API (api-sales-gamma.vercel.app)
        │  GET /ventas?desde=&hasta=  (autenticado con API key, Databricks Secrets)
        ▼
   landing (Volumen)      JSON crudo, un archivo por rango de fechas
        ▼
   bronze.ventas           Todo en STRING, inmutable, INSERT INTO (append)
        ▼
   silver.ventas           Limpio, tipado, deduplicado por hash — MERGE
        ▼
   gold.*                  Esquema estrella + junk dimension — MERGE / SCD Tipo 2
        ▼
   semantica.*              4 vistas de solo lectura para negocio
```

| Capa | Tecnología | Estrategia de carga |
|---|---|---|
| Landing | Volumen de Unity Catalog | Archivo nombrado por rango de fechas — determinista, no acumula copias |
| Bronze | Delta, todo STRING | `INSERT INTO`, a propósito no idempotente (silver limpia los duplicados) |
| Silver | Delta, tipado | `MERGE` con dedup por hash de la línea de negocio completa |
| Gold | Delta, modelo dimensional | `INSERT OVERWRITE` (dim_tiempo) / `MERGE` (SCD 1 y junk dim) / MERGE + INSERT en dos pasos (SCD 2) |
| Semántica | Vistas | Solo lectura sobre gold |

## Modelo dimensional

| Tabla | Tipo | Grano / SCD |
|---|---|---|
| `dim_tiempo` | Dimensión | SCD 0 — generada una sola vez |
| `dim_sucursal` | Dimensión | SCD 1 — 5 sucursales fijas |
| `dim_producto` | Dimensión | SCD 2 sobre `precio_lista` |
| `dim_empleado` | Dimensión | SCD 2 sobre la sucursal asignada |
| `dim_transaccion` | Junk dimension | `metodo_pago` + `estado_venta` combinados |
| `fact_ventas` | Hechos | Una fila por línea de venta válida. `ticket_id` como dimensión degenerada |

`precio_lista` (dimensión, cambia cada 12-25 días) y `precio_unitario` (métrica de la fact, cambia en cada venta según el descuento) son campos distintos a propósito — la diferencia entre ambos es el descuento real aplicado en esa venta.

## Cómo correrlo

### Día 0 — una sola vez

1. **Verificar identidad con LinkedIn** en Databricks Free Edition (perfil → verificación), si todavía no está hecho — sin esto, el acceso saliente a internet está restringido y la ingesta no va a poder llamar a la API.
2. Conectar este repo como **Git folder** en el Workspace de Databricks.
3. Correr `src/etl/etl_setup_secret_scope.py` **a mano, una sola vez** (nunca se agenda en el Job): crea el secret scope `kiosco_secrets` y guarda ahí la API key de `api_sales`.

### Orden de los notebooks

1. Todo `src/ddl/*.sql`, en cualquier orden (usan `IF NOT EXISTS`).
2. `src/etl/etl_extraer_api_a_landing.py`
3. `src/etl/etl_landing_a_bronze.sql`
4. `src/etl/etl_calidad_bronze.sql`
5. (Opcional, exploratorio) `src/eda/eda_bronze_ventas.sql`
6. `src/etl/etl_bronze_a_silver.sql`
7. `src/etl/etl_calidad_silver.sql`
8. `etl_gold_dim_tiempo.sql`, `etl_gold_dim_sucursal.sql`, `etl_gold_dim_producto_scd2.sql`, `etl_gold_dim_empleado_scd2.sql`, `etl_gold_dim_transaccion.sql` — sin dependencia entre sí, se pueden correr en cualquier orden o en paralelo
9. `src/etl/etl_gold_fact_ventas.sql` (recién después de las 5 dimensiones)
10. `src/etl/etl_calidad_gold.sql`
11. Verificar las vistas de `semantica` (al final de `ddl_semantica_vistas.sql`)

### Workflow automatizado

Crear un Job en Databricks Workflows con una tarea por notebook, encadenadas según el orden de arriba (las 5 dimensiones como tareas paralelas dependiendo todas de `etl_calidad_silver`, y `etl_gold_fact_ventas` dependiendo de las 5). Programado diario. A cada tarea SQL se le pasa el parámetro `run_id` con el valor `{{job.run_id}}`. Cada tarea de `calidad_*` corta la cadena si falla.

## Por qué SQL puro

Toda la transformación está escrita en SQL de Databricks, sin PySpark ni `spark.sql()` desde Python. Las piezas que suelen resolverse con Python se hacen con SQL nativo:

| Necesidad | Cómo se resuelve en SQL |
|---|---|
| Leer JSON con schema forzado a STRING | `read_files(..., format => 'json', schema => '...')` |
| Saber de qué archivo vino cada fila | `_metadata.file_path` |
| Pasar el `run_id` del Job | `CREATE WIDGET` + `:run_id` |
| Cortar el Job si falla un control de calidad | `assert_true(condición, 'mensaje')` |
| Pasos intermedios validables | Vistas temporales, una por transformación |

Solo dos notebooks siguen en Python, porque hacen un pedido HTTP y SQL no puede llamar a una API externa: `etl_setup_secret_scope.py` (crea el secret scope) y `etl_extraer_api_a_landing.py` (pide los datos a la API y guarda el JSON tal cual). Ninguno de los dos transforma datos.

## Verificación

Se corrió el pipeline completo con Spark local y Delta, con modo ANSI activado igual que serverless, contra datos reales de la API (del 1 al 26 de septiembre de 2026):

| Resultado | Valor |
|---|---|
| Filas en bronze | 3.610 |
| Filas en silver (después de deduplicar) | 3.580 |
| Filas en `fact_ventas` | 3.505 |
| Excluidas por `cantidad <= 0` en ventas aprobadas | 75 |
| Productos con historial de precio (SCD 2) | 16 de 16 |
| Vendedores con traslado de sucursal (SCD 2) | 3 |
| Controles de calidad | Todos en verde |
| Segunda corrida completa | Mismos conteos (idempotente) |

**Limitación conocida:** la API no manda un número de línea dentro del ticket. Cuando un mismo ticket tiene dos líneas del mismo producto con la misma cantidad y el mismo descuento, silver no puede distinguirlas de un duplicado de ingesta y se queda con una sola. Medido contra el generador: de las 30 filas descartadas, 12 eran duplicados reales y 18 eran ventas legítimas (0,5% del total).

## Datos de la fuente

La API (repo aparte, [`api_sales`](https://github.com/celesia/api_sales)) genera ventas sintéticas pero deterministas por fecha: pedir el mismo día dos veces siempre da las mismas filas. Trae suciedad real a propósito — nombres de sucursal con mayúsculas/espacios inconsistentes, precios con coma decimal, campos nulos, cantidades inválidas y outliers estadísticos — para poder practicar limpieza de datos real, no simulada.
