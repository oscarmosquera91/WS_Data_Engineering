# 📗 Documentación Técnica – Notebook

---

## 1. Identificación

| Campo | Descripción |
|--------|--------------|
| **Nombre del Notebook** | `NB_<Función>_<Producto>` |
| **Módulo / Ruta** | Ejemplo: `/notebooks/ingestion/` |
| **Autor** | Nombre del responsable |
| **Versión** | v1.0.0 |
| **Fecha de creación** | AAAA-MM-DD |
| **Última actualización** | AAAA-MM-DD |
| **Notebook objetivo** | Ejemplo: `NB_Ingest_TiendasON` |

---

## 2. Descripción general

Breve descripción del propósito del notebook, su rol dentro del proceso de datos y la relación con otras capas (Bronze, Silver, Gold).

**Ejemplo:**
> Este notebook ejecuta la ingesta incremental de datos desde SQL Server hacia la capa Bronze del Lakehouse en Microsoft Fabric.  
> Controla watermark, registra logs y guarda los resultados en formato Delta.

---

## 3. Entradas / Salidas

| Tipo | Nombre | Descripción | Ubicación |
|------|---------|--------------|-----------|
| **Entrada** | SQL Server – Tabla `SalesHeaderAndDetails` | Datos de ventas crudos | Fuente |
| **Salida** | Delta – `bronze/sales_raw` | Registros limpios con control de watermark | Lakehouse |

---

## 4. Lógica técnica

1. Lectura de configuración desde `json_config`.
2. Extracción de datos usando `SQLAlchemy` y filtros de watermark.
3. Limpieza de datos con función `clean_data(df, schema)`.
4. Escritura en Delta Lake (`merge` o `overwriteDynamic`).
5. Registro de logs en tabla `Audit_Log`.

---

## 5. Parámetros

| Parámetro | Descripción | Tipo | Ejemplo |
|------------|-------------|------|----------|
| `project_name` | Nombre del proyecto a procesar | string | `"TiendasON"` |
| `table_name` | Nombre de la tabla a extraer | string | `"SalesHeaderAndDetails"` |
| `id_partner` | Identificador de la plataforma | int | `23` |

---

## 6. Dependencias

- **Funciones internas:**  
  `get_sql_table_schema`, `clean_data`, `save_to_delta`
- **Librerías externas:**  
  `pyspark.sql`, `sqlalchemy`, `pandas`, `delta.tables`
- **Archivos relacionados:**  
  `/config/json_config/ingest_config_tiendason.json`

---

## 7. Ejecución

El notebook puede ser ejecutado:
- Manualmente desde Fabric Notebook.
- Desde un **pipeline** mediante actividad “Notebook”.
- Como parte de un flujo programado en la aplicación FastAPI de control.

---

## 8. Seguridad

- Conexión a SQL Server mediante autenticación de servicio.
- No exponer credenciales directamente en el código.
- Accesos controlados mediante permisos del Lakehouse.

---

## 9. Versionamiento

- Seguir el esquema semántico `vX.Y.Z`.
- Registrar cambios funcionales y técnicos en `CHANGELOG.md`.

---

## 10. Métricas y observaciones

- Registros procesados.
- Tiempo total de ejecución.
- Observaciones sobre errores o ajustes pendientes.
