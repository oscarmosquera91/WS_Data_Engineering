# 📘 Estándares de Naming – WS_Data_Engineering

## 1. Objetivo
Unificar la nomenclatura de objetos, archivos y scripts en el proyecto para mantener coherencia, trazabilidad y automatización.

---

## 2. Convenciones generales
| Tipo | Formato | Ejemplo |
|------|----------|----------|
| **Notebooks** | `NB_<Función>_<Producto>` | `NB_Ingest_TiendasON` |
| **Pipelines** | `PL_<Función>_<Producto>` | `PL_Transform_Sales` |
| **Tablas Lakehouse** | `tbl_<Nombre>` | `tbl_Sales_Header` |
| **Funciones Python** | `snake_case` | `get_table_schema()` |
| **Clases Python** | `CamelCase` | `DataIngestManager` |
| **Variables DataFrame** | `df_<Nombre>` | `df_sales_clean` |
| **Configuraciones JSON** | `cfg_<Nombre>` | `cfg_ingest_sales` |

---

## 3. Carpeta `/docs`
| Archivo | Propósito |
|----------|------------|
| `01_Alcance_Proyecto.md` | Define objetivos y alcance general |
| `02_Estandares_Naming.md` | Reglas de nomenclatura |
| `03_Estructura_Notebooks.md` | Guía para notebooks técnicos |
| `04_Estructura_Pipelines.md` | Guía para pipelines |
| `05_Guia_Buenas_Prácticas.md` | Recomendaciones y estándares |

---

## 4. Logs
Formato sugerido:

```text
[INFO] - 2025-10-27 10:00:00 - NB_Ingest_TiendasON - Ingesta completada exitosamente
[ERROR] - 2025-10-27 10:01:00 - NB_Ingest_TiendasON - Error al conectar con SQL Server
```

---

## 5. Versionamiento
- Seguir versión semántica: `v1.0.0`
- Documentar cambios relevantes en `CHANGELOG.md`
- Los notebooks deben incluir su versión en la sección de identificación.

---

## 6. Estructura de carpetas sugerida

```text
notebooks/
├─ NB_Ingest_ProductoA.ipynb
├─ NB_Transform_ProductoA.ipynb
├─ NB_Gold_Model_ProductoA.ipynb

pipelines/
├─ PL_Ingest_ProductoA.json
└─ PL_Gold_Model_ProductoA.json
```

