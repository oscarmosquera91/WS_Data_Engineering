# 🧭 Guía de Buenas Prácticas – WS_Data_Engineering

---

## 1. Objetivo
Establecer lineamientos técnicos y metodológicos para el desarrollo de notebooks, pipelines y procesos dentro de **Microsoft Fabric**, asegurando consistencia, mantenibilidad y desempeño.

---

## 2. Estructura general de carpetas

```text
📦 WS_Data_Engineering/
├─ .github/
│  ├─ COPILOT_INSTRUCTIONS.md
│  ├─ ISSUE_TEMPLATE/
│  │   ├─ bug_report.md
│  │   └─ feature_request.md
│  ├─ PULL_REQUEST_TEMPLATE.md
│  └─ workflows/
│      └─ ci-pipeline.yml
│
├─ docs/
│  ├─ 01_Alcance_Proyecto.md
│  ├─ 02_Estandares_Naming.md
│  ├─ 03_Estructura_Notebooks.md
│  ├─ 04_Estructura_Pipelines.md
│  └─ 05_Guia_Commit_y_Versionado.md
│
├─ src/
│  ├─ ingestion/
│  ├─ transform/
│  ├─ standardization/
│  └─ gold/
│
├─ tests/
│  └─ unit/
│
├─ README.md
├─ requirements.txt
└─ .gitignore

```


---

## 3. Control de versiones
- Utilizar ramas descriptivas:
  - `main`: rama estable.
  - `dev`: rama de desarrollo.
  - `feature/<nombre>`: nuevas funciones.
  - `hotfix/<nombre>`: correcciones críticas.
- Versionar notebooks y pipelines con formato `vX.Y.Z`.
- Registrar cambios en `CHANGELOG.md`.

---

## 4. Logs y auditoría
- Usar el formato estándar de logs:
```text
[INFO] - 2025-10-27 10:00:00 - NB_Ingest_TiendasON - Ingesta completada exitosamente
[ERROR] - 2025-10-27 10:01:00 - NB_Ingest_TiendasON - Error al conectar con SQL Server
```
- Mantener tabla centralizada `Audit_Log` con los campos:
- `operation`, `table_name`, `primary_key`, `field`, `old_value`, `new_value`, `timestamp`, `user`
- Evitar `print()` en notebooks; usar `logging` o `log4j` cuando aplique.

---

## 5. Control de errores
- Usar bloques `try/except` con manejo de errores específicos.
- Registrar errores críticos con detalle de stacktrace.
- Incluir fallback (por ejemplo, “retry connection”) cuando sea posible.
- Evitar detener el pipeline completo si una tabla falla: aislar procesos.

---

## 6. Performance y optimización
- Repartir datos grandes usando `repartition()` o `coalesce()`.
- Evitar acciones costosas (`collect()`, `toPandas()`) en datasets grandes.
- Usar formatos **Delta** y **Parquet** según la etapa.
- En joins, preferir `broadcast()` cuando una tabla es pequeña.
- Aplicar `optimize` y `vacuum` periódicamente en Delta Lake.

---

## 7. Seguridad y configuración
- No incluir credenciales en notebooks ni pipelines.
- Usar **Microsoft Entra ID** o **Service Principals** para conexiones.
- Centralizar credenciales en **Fabric Linked Connections**.
- Proteger las rutas del Lakehouse según capas:
- Bronze: acceso restringido a desarrolladores.
- Silver y Gold: lectura controlada para analistas.

---

## 8. Documentación
- Cada notebook y pipeline debe documentarse según los formatos de `/docs`.
- Mantener autor, versión y fecha actualizada.
- Incluir comentarios inline en código con formato:
```python
# 🔹 Paso 1: Lectura incremental desde SQL Server
```

---

## 9. Métricas de ejecución
- Registrar en logs:
  - Tiempos de inicio y fin
  - Cantidad de registros procesados
  - Duración total
- Mantener una tabla histórica de ejecuciones (opcional):
`Execution_Log (process_name, start_time, end_time, status, records, duration)`

---

## 10. Recomendaciones de desarrollo
- Reutilizar funciones comunes (crear módulo utils/).
- Mantener notebooks con menos de 500 líneas; dividir por procesos.
- Versionar notebooks exportándolos en .ipynb y .py para trazabilidad.
- Validar tipos de datos antes de escribir en Delta.

---

## 11. Escalabilidad futura
- Implementar orquestación dinámica con FastAPI + APScheduler.
- Evaluar uso de Fabric Data Activator o Synapse Dataflows para automatizaciones.
- Adoptar patrones de ingestión modular (por dominio o producto).
