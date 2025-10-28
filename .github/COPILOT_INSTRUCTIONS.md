# 🤖 Instrucciones para GitHub Copilot

## 🧩 Contexto del proyecto
Este repositorio hace parte del workspace **WS_Data_Engineering**, orientado al desarrollo de procesos de **ingeniería de datos en Microsoft Fabric**.  
Los módulos principales incluyen:
- **Ingesta**: extracción desde múltiples bases SQL Server.
- **Transformación**: estandarización de datos con PySpark y Pandas.
- **Modelado**: aplicación de la arquitectura *Medallion* (Bronze, Silver, Gold).
- **Orquestación**: ejecución mediante Pipelines y Notebooks.

El entorno de ejecución utiliza:
- **Microsoft Fabric / OneLake**
- **Delta Lake**
- **PySpark y Pandas**
- **SQLAlchemy (para lectura desde SQL Server)**
- **Control de watermark y auditoría en Delta**

---

## 🎯 Objetivo
Copilot debe asistir en la generación de código, documentación y buenas prácticas para:
1. Procesos de ingesta, transformación y consolidación.
2. Lógica de lectura/escritura Delta Lake.
3. Estructuras de control y logging.
4. Documentación técnica de Notebooks y Pipelines.

---

## 🧠 Reglas para Copilot Chat
- Siempre asumir el contexto de Microsoft Fabric (Spark + Lakehouse).
- Usar convenciones de nombres definidas en `docs/02_Estandares_Naming.md`.
- Al generar código, incluir manejo de errores y logs.
- Usar formato de log:
```text
[INFO] - 2025-10-27 10:00:00 - NB_Ingest_TiendasON - Ingesta iniciada
```
- Los ejemplos deben ser **compatibles con Notebooks de Fabric**.
- No usar librerías que no estén soportadas por Fabric.
- Priorizar las respuestas en español

---

## 📄 Documentación
- Cada Notebook debe documentarse con la plantilla:  
**Documentación Técnica – Notebook**
- Cada Pipeline debe documentarse con:  
**Documentación Técnica – Pipeline**

---

## ⚙️ Ejemplo de comportamiento esperado
Copilot debe:
- Sugerir código de lectura incremental con watermark.
- Explicar cómo escribir en Delta con `merge` o `overwriteDynamic`.
- Usar `display(df)` o `df.show()` para inspección de datos.
- Recomendar optimizaciones de rendimiento y paralelismo en Fabric.

