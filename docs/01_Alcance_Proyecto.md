# 📘 Alcance del Proyecto – WS_Data_Engineering

## 1. Descripción general
El proyecto **WS_Data_Engineering** implementa una arquitectura de datos en Microsoft Fabric, orientada a la integración de información desde múltiples plataformas comerciales y operativas.  
El objetivo principal es centralizar, estandarizar y disponibilizar datos en **OneLake** usando **Delta Lake** bajo el modelo **Medallion Architecture (Bronze, Silver, Gold)**.

---

## 2. Objetivos específicos
1. Diseñar pipelines y notebooks para ingesta automatizada desde SQL Server.
2. Implementar procesos de transformación y homologación de datos.
3. Estandarizar estructuras en capas Silver y Gold.
4. Habilitar reporting mediante Power BI conectado a Fabric.
5. Asegurar trazabilidad y auditoría mediante logs centralizados.

---

## 3. Alcance funcional
| Capa | Descripción | Tecnologías |
|------|--------------|--------------|
| **Bronze** | Datos crudos desde orígenes | SQL Server, Fabric Pipelines |
| **Silver** | Datos limpios y estandarizados | PySpark, Delta Lake |
| **Gold** | Datos analíticos y dimensionales | Fabric Lakehouse, Power BI |

---

## 4. Lineamientos
- Todos los scripts deben ser compatibles con **Fabric Notebooks (Spark Runtime)**.
- Los procesos deben manejar **control incremental (watermark)**.
- El almacenamiento se organiza por producto o dominio (ej. TiendasON, RutasON, TryController).
- Se aplican convenciones de nombres del documento `02_Estandares_Naming.md`.
- La documentación técnica se mantiene junto al código en `/docs`.

---

## 5. Responsabilidades
- **Data Engineer**: desarrollo técnico de notebooks y pipelines.
- **Data Architect**: revisión de modelos y estructuras Delta.
- **Ops Engineer**: monitoreo de ejecución y recursos Fabric.

