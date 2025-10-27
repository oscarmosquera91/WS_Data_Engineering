# 📘 Documentación Técnica – Notebook: NB_Ingest_TiendasON

**Autor:** Oscar Mosquera  
**Versión:** 1.1  
**Fecha:** 2025-10-09  
**Notebook objetivo:** `NB_Ingest_TiendasON`  

---

## 1. Identificación

| Campo | Descripción |
|--------|--------------|
| **Nombre del Notebook** | `NB_Ingest_TiendasON` |
# 📘 Documentación Técnica – Notebook: NB_Ingest_TiendasON

**Autor:** Oscar Mosquera  
**Versión:** 1.2  
**Fecha:** 2025-10-09  
**Notebook objetivo:** `NB_Ingest_TiendasON`  

---

## 1. Identificación

| Campo | Descripción |
|--------|--------------|
| **Nombre del Notebook** | `NB_Ingest_TiendasON` |
| **Módulo / Capa Medallion** | Ingestion / Bronze |
| **Workspace** | WS_Data_Engineering |
| **Lakehouse** | LH_TiendasOn |
| **Autor / Responsable** | Oscar Mosquera |
| **Versión** | 1.2 |
| **Fecha de creación / última modificación** | 2025-10-09 |
| **Estado** | En revisión |

---

## 2. Propósito y alcance

- **Propósito:** Orquestar la extracción incremental de datos de tiendas desde fuentes SQL/OLTP hacia el Lakehouse, aplicando control de watermark, particionado por ventanas horarias y registros de auditoría.
- **Alcance:** Este documento describe únicamente `NB_Ingest_TiendasON`. Aunque el notebook utiliza utilidades y módulos compartidos (por ejemplo `db_utils`, `partition_utils`, `watermark_utils`, `sql_repository`), la documentación se centra en la lógica, entradas y ejecución del notebook.
- **Salida principal:** Tablas delta en la zona `bronze` por tienda/plataforma.

---

## 3. Entradas y salidas

| Tipo | Recurso | Descripción |
|------|----------|--------------|
| **Entrada (Scheduler/Trigger)** | Scheduler / Pipeline trigger | Indica cuándo ejecutar el notebook y parámetros runtime (p.ej. `store_id`, `run_id`, `partition_window`, overrides). |
| **Entrada (Config)** | `config.py` / JSON externos / `get_schedules` | Parámetros de conexión, artefactos y reglas de particionado. En este notebook la lista de programaciones se obtiene mediante `get_schedules(conn)` desde la base central y se filtra por `project` para determinar qué jobs ejecutar. |
| **Entrada (Secrets)** | Key Vault / Secret Store | Credenciales para conexiones a bases de datos y artefactos. |
| **Salida** | `lakehouse.bronze.*` | Tablas Delta destino (por dataset/plataforma) |
| **Log / Auditoría** | Tabla `Ingest_Audit` / Log files | Estado, filas procesadas, duración, errores |

---

## 4. Estructura técnica y pasos clave

1. Inicialización: carga de `config.py`, logger y utilidades (`db_utils`, `partition_utils`, `watermark_utils`, `sql_repository`).
2. Lectura de programaciones: invocar `get_schedules(central_conn)` para obtener el catálogo de jobs y filtrar por `project`.
3. Determinación de la franja horaria y cálculo del bloque actual (usando `get_block_number_in_window`).
4. Para cada bloque/plataforma: resolver artefacto/linked service y obtener conexión desde `FabricConnectionManager` (a través de `ConnectionPool`).
5. Lectura incremental con watermark (si existe) o carga completa según parámetros.
6. Escritura a Delta por partición y actualización del watermark en tabla de tracking.
7. Registro en `Ingest_Audit`, liberación de recursos y metrics.

### Notas importantes
- El notebook consulta `get_schedules` para decidir qué programas/jobs ejecutar. `get_schedules` retorna metadata (project, source_type, table_name, schedule_time, interval, etc.) que el notebook filtra por `project` y `source_type`.
- El notebook usa particionado por bloque horario; cuando `get_block_number_in_window` devuelve `None` se omite el procesamiento para ese run.
- El manejo correcto de conexiones y liberación es crítico: usa `acquire_connection_context` o asegúrate de `release_connection` en finally.

---

## 5. Ejemplo de resultado de `get_schedules`

Payload mínimo esperado (ejemplo):

```json
[
  {
    "project": "TiendasOn",
    "source_type": "platform",
    "platform_name": "TryController",
    "table_name": "DevicesRoutesTokenSession",
    "schedule_time": "00:00",
    "interval": "hourly",
    "enabled": true
  },
  {
    "project": "TryController",
    "source_type": "database",
    "platform_name": null,
    "table_name": "Loans",
    "schedule_time": "11:00",
    "interval": "daily",
    "enabled": true
  }
]
```

Columnas relevantes que el notebook utiliza: `project`, `source_type`, `table_name`, `schedule_time`, `enabled`, `platform_name`.

---

## 5.1 Validación del payload de `get_schedules`

Es recomendable validar el payload que retorna `get_schedules` antes de usarlo en producción. A continuación se especifica el schema esperado y ejemplos de estructuras para `keys_info` y `partition_info`.

- Columnas mínimas obligatorias (DataFrame):
  - `tables_source_id` (int)
  - `project` (str)
  - `source_type` (str) - valores esperados: `platform`, `database`, etc.
  - `table_name` (str)
  - `execution_time` (HH:MM o HH:MM:SS)
  - `status` (0/1)

- Columnas opcionales (útiles):
  - `keys_info`: lista de objetos [{"field_name": "id", "is_primary_key": 1}, ...]
  - `partition_info`: lista de objetos [{"field_name": "created_at", "type_field": "datetime"}, ...]
  - `is_incremental` (0/1 o true/false)
  - `platform_name`, `schem`, `enabled`, `interval`

Ejemplo de `keys_info` y `partition_info`:

```python
keys_info = [
    {"field_name": "id", "is_primary_key": 1},
    {"field_name": "store_id", "is_primary_key": 0},
]

partition_info = [
    {"field_name": "CreatedTS", "type_field": "datetime"},
]
```

Snippet para validar desde el notebook usando el validador incluido en el repo (`validate_get_schedules.py`):

```python
from validate_get_schedules import validate_schedules_from_conn

# conn: conexión/engine que acepta get_schedules
ok, messages, df_schedules = validate_schedules_from_conn(conn, allow_import=True, strict=True)
if not ok:
    log("Payload de get_schedules inválido:\n" + "\n".join(messages), level="error")
    raise RuntimeError("Validación de get_schedules falló; revisar mensajes de log.")
else:
    log("Validación de payload OK", level="info")

```

Notas:
- `allow_import=True` permite que el helper importe `ingestion_utils.get_schedules` directamente; úsalo con cuidado en entornos donde la importación pueda cargar dependencias pesadas.
- `strict=True` activa validaciones adicionales que verifican la estructura de `keys_info` y `partition_info` por fila.


## 6. Parámetros de configuración (ejemplos)

```python
# config.py (extracto)
NOTEBOOK_NAME = "NB_Ingest_TiendasON"
WORKSPACE_ID = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
ARTIFACT_SQL = "DB_Engineering"
POOL_MAX_CONNECTIONS = 10
PARTITION_TOTAL_BLOCKS = 4
PARTITION_WINDOW_START = 0   # 00:00
PARTITION_WINDOW_END = 8     # 08:00
WATERMARK_TABLE = "ingest.watermarks"
LOG_TABLE = "DataOn.LastWatermarkState"
```

---

## 7. Snippets de uso y ejemplos prácticos

### 7.1 Leer programaciones y filtrar por proyecto

```python
# central_db_conn: conexión a la base central con la tabla de schedules
from sql_repository import get_schedules
 
df_schedules = get_schedules(central_db_conn)
# filtrar por project y tipo
df_pf_TiendasOn = df_schedules[(df_schedules["source_type"].str.lower() == "platform") &
                                (df_schedules["project"] == "TiendasOn")]
```

### 7.2 Determinar bloque horario y procesar

```python
block_number = get_block_number_in_window(PARTITION_TOTAL_BLOCKS, PARTITION_WINDOW_START, PARTITION_WINDOW_END)
if block_number is None:
    log("Fuera de la franja, no se procesa este run", level="info")
else:
    # procesar solo los registros/plataformas de este bloque
    df_block_conns = get_block(df_total, block_number, PARTITION_TOTAL_BLOCKS)
```

### 7.3 Adquirir conexión segura con timeout

```python
with conn_mgr_fabric.connection_pool.acquire_connection_context(f"sql_{ARTIFACT_SQL}", lambda: get_db_connection_fabricsqldatabase2(ARTIFACT_SQL, WORKSPACE_ID, notebookutils), wait_for=30) as conn:
    if conn is None:
        log("No hay conexión disponible, abortando run", level="error")
    else:
        # leer / procesar
        df = fetch_all_data(conn, query)
```

---

## 8. Troubleshooting (problemas comunes)

- Error: `No se pudo obtener conexión ... Pool lleno.`
  - Causa: pool alcanzó `max_connections` y no hay conexiones libres.
  - Acciones: aumentar `POOL_MAX_CONNECTIONS`, asegurar `release_connection` en finally/context manager, reducir concurrencia, revisar que `connection_key` es único por artifact.

- Error: `get_block_number_in_window` devuelve `None` inesperadamente
  - Causa: hora actual fuera de la franja configurada o franja mal configurada (p. ej. start==end se interpreta como vacía).
  - Acciones: verificar `PARTITION_WINDOW_START`/`END`, zona horaria y uso de `datetime.now() - timedelta(hours=offset)` si aplica; considerar cambiar la interpretación de start==end a 24h si lo deseas.

- Error: `KeyError` o columnas faltantes al aplicar `get_block` / `get_sql_table_schema`
  - Causa: mismatch entre metadata de `get_schedules` y la tabla real.
  - Acciones: inspeccionar `df_schedules` y `df_schema`, loggear columnas y ejemplos de filas.

---

## 9. Observaciones y próximos pasos

- Añadir métricas de pool y trazabilidad por conexión (logs periódicos de `pool.get_stats()`).
- Añadir tests unitarios para `get_block_number_in_window` y pruebas de integración para la lógica de conexión y `get_schedules`.
- Documentar el payload del scheduler y la forma en que pipeline pasa `run_id`, `store_id` y `overrides` al notebook.

---

**Fin de la documentación actualizada para `NB_Ingest_TiendasON`.**
