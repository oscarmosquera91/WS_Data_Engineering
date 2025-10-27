# 📙 Documentación Técnica – Pipeline

---

## 1. Identificación

| Campo | Descripción |
|--------|--------------|
| **Nombre del Pipeline** | `PL_<Función>_<Producto>` |
| **Módulo / Ruta** | Ejemplo: `/pipelines/ingestion/` |
| **Autor** | Nombre del responsable |
| **Versión** | v1.0.0 |
| **Fecha de creación** | AAAA-MM-DD |
| **Última actualización** | AAAA-MM-DD |
| **Pipeline objetivo** | Ejemplo: `PL_Ingest_TiendasON` |

---

## 2. Descripción general

Explica la finalidad del pipeline y su rol dentro del proceso de datos.  
**Ejemplo:**
> Pipeline encargado de ejecutar la ingesta diaria de datos desde SQL Server hacia la capa Bronze.  
> Controla ejecución secuencial de notebooks y registra logs de actividad.

---

## 3. Estructura y componentes

| Tipo | Nombre | Descripción |
|------|----------|-------------|
| **Notebook Activity** | `NB_Ingest_TiendasON` | Realiza la extracción de datos |
| **Dataflow Activity** | `DF_CleanSales` | Limpieza intermedia |
| **Copy Data Activity** | `Copy_To_Bronze` | Copia final a Delta Lake |
| **Wait Activity** | `Wait_5_Min` | Sincronización entre pasos |

---

## 4. Parámetros

| Parámetro | Descripción | Tipo | Ejemplo |
|------------|-------------|------|----------|
| `date_start` | Fecha inicial del rango | date | `"2025-10-01"` |
| `date_end` | Fecha final del rango | date | `"2025-10-27"` |
| `id_partner` | Identificador de la plataforma | int | `23` |

---

## 5. Entradas / Salidas

| Tipo | Nombre | Descripción | Ubicación |
|------|----------|--------------|-----------|
| **Entrada** | `SQL Server – ventas` | Datos fuente crudos | Origen |
| **Salida** | `bronze/sales_raw` | Datos en formato Delta | Lakehouse |

---

## 6. Dependencias

- **Notebook dependiente:** `NB_Ingest_TiendasON`
- **Pipeline anterior:** `PL_Transform_TiendasON`
- **Actividades adicionales:** alertas o limpieza de staging.

---

## 7. Ejecución

- Configurado para ejecución diaria (cada 24h).
- Puede ser lanzado desde:
  - Microsoft Fabric
  - Aplicación FastAPI con programación APScheduler

---

## 8. Monitoreo y control

- Logs almacenados en tabla `Audit_Log`.
- Errores críticos notificados por correo o Teams (futuro).
- Métricas monitoreadas: duración, número de registros, errores.

---

## 9. Seguridad

- Acceso mediante **Service Principal**.
- Restricción a roles definidos en Fabric.
- No incluir secretos en parámetros del pipeline.

---

## 10. Versionamiento

- Seguir convención semántica `vX.Y.Z`.
- Actualizar `CHANGELOG.md` con cada modificación funcional.

---

## 11. Métricas y observaciones

- Promedio de tiempo por ejecución.
- Número de actividades ejecutadas.
- Registros procesados exitosamente.
