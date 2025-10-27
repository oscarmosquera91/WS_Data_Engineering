import pandas as pd
from logger_utils import log
from format_utils import format_datetime_for_sqlserver

def get_all_last_watermarks(resource_project, _conn_mgr_fabric, _environment, _log_table):
    """
    Obtiene todos los registros de la tabla DataOn.LastWatermarkState y los devuelve como un DataFrame.
    Retorna None en los siguientes casos:
    1. Si no hay resultados en la consulta
    2. Si hay un error en la ejecución
    3. Si el DataFrame está vacío después de filtrar
    """
    try:
        query = f"""
            SELECT 
                project,
                idPartner,
                TableName,
                environment,
                Status,
                is_incremental,
                CAST(lastWatermarkValue AS varchar(30)) AS lastWatermarkValue,
                CAST(currentWaterMarkValue AS varchar(30)) AS currentWaterMarkValue
            FROM {_log_table}
            WHERE Status = 'Success'
              AND project = '{resource_project}'
              AND is_incremental = 1
              AND environment = '{_environment}'
        """
        df_watermarks = pd.read_sql(query, _conn_mgr_fabric.get_sql_connection())
        
        # Si no hay resultados, retornar None
        if df_watermarks.empty:
            log(f"⚠️ No se encontraron watermarks en {_log_table} para project={resource_project}, environment={_environment}", level="warning")
            return None

        # Agregar índices para optimizar búsquedas
        df_watermarks.set_index(['idPartner', 'TableName', 'project'], inplace=True)
        df_watermarks.sort_index(inplace=True)
        
        # Verificar si hay registros después de filtrar e indexar
        if len(df_watermarks) == 0:
            log(f"⚠️ No hay watermarks válidos después de filtrar", level="warning")
            return None
            
        log(f"✅ Se obtuvieron {len(df_watermarks)} registros de watermarks", level="info")
        return df_watermarks

    except Exception as e:
        log(f"❌ Error al obtener todos los watermarks: {str(e)}", level="error")
        return None

def get_last_watermark_from_cache(_project, df_watermarks, _id_partner, _table_name, _watermark_type):
    """
    Obtiene el último watermark para una tabla específica desde el DataFrame cacheado.
    Siempre retorna un valor válido (nunca None):
    - Para DATETIME: '1990-01-01 00:00:00' formateado
    - Para INT: 0
    """
    try:
        # Valor por defecto según el tipo
        default_value = format_datetime_for_sqlserver('1990-01-01 00:00:00') if _watermark_type.upper() == "DATETIME" else 0

        # Validar el tipo de watermark
        if _watermark_type.upper() not in ["DATETIME", "INT"]:
            log(f"❌ Tipo de watermark no soportado: {_watermark_type}", level="error")
            return default_value

        # Verificar si df_watermarks es None o está vacío
        if df_watermarks is None or df_watermarks.empty:
            log(f"⚠️ DataFrame de watermarks vacío o None, usando valor por defecto", level="warning")
            return default_value

        # Intentar obtener el registro específico usando los índices
        try:
            # Filtrar usando valores del índice con los tres componentes
            mask = (df_watermarks.index.get_level_values('idPartner') == int(_id_partner)) & \
                  (df_watermarks.index.get_level_values('TableName') == _table_name) & \
                  (df_watermarks.index.get_level_values('project') == _project)
            watermark_rows = df_watermarks[mask]
            
            if watermark_rows.empty:
                log(f"⚠️ No se encontraron watermarks para partner={_id_partner}, tabla={_table_name}, del proyecto={_project}, usando valor por defecto", level="warning")
                return default_value
                
            if len(watermark_rows) == 1:
                last_watermark_raw = watermark_rows['currentWaterMarkValue'].iloc[0]
                log(f"✅ Watermark encontrado para partner={_id_partner}, tabla={_table_name}", level="info")
            else:
                # Si hay múltiples registros, tomar el más reciente
                last_watermark_raw = watermark_rows.sort_values('currentWaterMarkValue', ascending=False)['currentWaterMarkValue'].iloc[0]
                log(f"✅ Se encontraron múltiples watermarks, usando el más reciente para partner={_id_partner}, tabla={_table_name}", level="info")

            # Verificar si el valor es None o vacío
            if pd.isna(last_watermark_raw) or last_watermark_raw == '' or last_watermark_raw is None:
                log(f"⚠️ Watermark encontrado es nulo o vacío, usando valor por defecto", level="warning")
                return default_value

            # Procesar el valor según el tipo
            if _watermark_type.upper() == "DATETIME":
                return format_datetime_for_sqlserver(last_watermark_raw)
            else:  # INT
                return int(last_watermark_raw)

        except (KeyError, IndexError):
            log(f"⚠️ No se encontró watermark para partner={_id_partner}, tabla={_table_name}, usando valor por defecto", level="warning")
            return default_value

    except Exception as e:
        log(f"❌ Error al obtener el last_watermark desde cache: {str(e)}", level="error")
        return default_value  # Retorna el valor por defecto en lugar de None
