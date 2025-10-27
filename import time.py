import time 
import pandas as pd
import numpy as np
from logger_utils import log, set_logging
from logging_utils import log_operation
from format_utils import format_datetime_for_sqlserver, sanitize_for_pandas, pandas_time_to_str, clean_data
from db_utils import create_db_connection
from partition_utils import get_batches, get_block_number, get_block
from concurrent.futures import ThreadPoolExecutor, as_completed
import gc
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from collections import defaultdict
import datetime  # módulo completo → para datetime.datetime y datetime.time
import uuid
from watermark_utils import get_all_last_watermarks, get_last_watermark_from_cache




# Función para consultar datos a procesar desde el WH
def get_schedules(_con):
    """ Obtiene los procesos procesos programados a jecutar segun el corte del dia (mañana/tarde). independiente del proyecto con sus repectivos parametros 
        para el procesamiento de los datos.
        La consulta se realiza sobre un warehouse
    Args:
        work_space_id: Id del workspace  donse se encuentra el warehouse con la información
        warehouse_name:  Nombre del warehouse donde se enceueta la información
        Returns:
        DataFrame: Datafren con las programaciones.
    """
    try:
        log("✅ Obtiene los procesos programados según el corte del día.", level="info")

        # 1️⃣ Leer en Pandas

        df_schedules = pd.read_sql("SELECT * FROM DataOn.VwGetSchedule;", _con)
        df_keysByTable = pd.read_sql("SELECT * FROM DataOn.keysByTable;", _con)
        df_PartitionFields = pd.read_sql("SELECT * FROM DataOn.PartitionFields;", _con)

        # 2️⃣ Limpieza de columnas tipo datetime.time
        df_schedules = sanitize_for_pandas(pandas_time_to_str(df_schedules))
        df_keysByTable = sanitize_for_pandas(pandas_time_to_str(df_keysByTable))
        df_PartitionFields = sanitize_for_pandas(pandas_time_to_str(df_PartitionFields))

        # 3️⃣ Filtrar por rango horario (corte del día)
        offset_hours = 5
        total_blocks = 4
        reference_datetime = datetime.datetime.now() - datetime.timedelta(hours=offset_hours)
        current_block = get_block_number(total_blocks)
        hours_per_block = 24 // total_blocks
        
        start_execution_hour = current_block * hours_per_block
        end_execution_hour = start_execution_hour + hours_per_block - 1

        # Convertir execution_time a datetime para filtrar
        df_schedules["execution_time_dt"] = pd.to_datetime(
            df_schedules["execution_time"], format="%H:%M:%S", errors="coerce"
        )
        df_schedules_filtered = df_schedules.loc[
            df_schedules["execution_time_dt"].dt.hour.between(start_execution_hour, end_execution_hour)
        ]

        # 4️⃣ Agrupar claves por tabla
        df_keysByTable_grouped = (
            df_keysByTable[df_keysByTable["status"] == 1]
            .groupby("tables_source_id")
            .apply(lambda g: g[["field_name", "is_primary_key"]].to_dict(orient="records"))
            .reset_index(name="keys_info")
        )

        # 5️⃣ Agrupar campos de partición
        df_PartitionFields_grouped = (
            df_PartitionFields[df_PartitionFields["status"] == 1]
            .groupby("tables_source_id")
            .apply(lambda g: g[["field_name", "type_field"]].to_dict(orient="records"))
            .reset_index(name="partition_info")
        )

        # 6️⃣ Unir resultados
        df_result = df_schedules_filtered.merge(df_keysByTable_grouped, on="tables_source_id", how="left") \
                                         .merge(df_PartitionFields_grouped, on="tables_source_id", how="left")
        
        # 7️⃣ Conversión explícita de is_incremental a booleano
        if "is_incremental" in df_result.columns:
            df_result["is_incremental"] = (
                df_result["is_incremental"]
                .astype(str)
                .str.strip()
                .str.lower()
                .map({"1": True, "0": False, "true": True, "false": False})
            )

        log("✅ Se obtuvieron los horarios de ejecución correctamente.", level="info")
        return df_result

    except Exception as e:
        log(f"❌ Error al obtener los horarios: {e}", level="error")
        return None


def get_last_watermark(_project, _id_partner, _table_name, _watermark_type, _conn_mgr_fabric, _environment, _log_table, _df_watermarks=None):
    """
    Obtiene el último watermark para una tabla específica.
    Si se proporciona _df_watermarks, obtiene el watermark desde el DataFrame cacheado,
    de lo contrario hace una consulta individual a la base de datos.
    """
    
    
    try:
        if _df_watermarks is not None:
            return get_last_watermark_from_cache(_project, _df_watermarks, _id_partner, _table_name, _watermark_type)
        
        # Si no hay DataFrame cacheado, hacer la consulta individual
        conn = _conn_mgr_fabric.get_sql_connection()
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT TOP 1 CAST(currentWaterMarkValue AS varchar(30)) AS lastWatermarkValue
                FROM {_log_table}
                WHERE idPartner = {_id_partner}
                  AND TableName = '{_table_name}'
                  AND environment = '{_environment}'
                  AND Project = '{_project}'
                  AND Status = 'Success'
                  AND is_incremental = 1
                ORDER BY currentWaterMarkValue DESC 
            """)
            rows = cursor.fetchall()

        # Manejo del watermark
        if rows:
            last_watermark_raw = rows[0][0]  # Esto puede ser datetime o int
            if _watermark_type.upper() == "DATETIME":
                last_watermark_value = str(last_watermark_raw)  # Devuelve '2025-04-04 14:53:22'
                last_watermark_value = format_datetime_for_sqlserver(last_watermark_value)
            elif _watermark_type.upper() == "INT":
                last_watermark_value = int(last_watermark_raw)
            else:
                raise ValueError(f"❌ Tipo de watermark no soportado: {_watermark_type}")
        else:
            # Valor por defecto si no hay registros
            if _watermark_type.upper() == "DATETIME":
                last_watermark_value = '1990-01-01 00:00:00'
                last_watermark_value = format_datetime_for_sqlserver(last_watermark_value)
            elif _watermark_type.upper() == "INT":
                last_watermark_value = 0
            else:
                raise ValueError(f"❌ Tipo de watermark no soportado: {_watermark_type}")
        return last_watermark_value
    except Exception as e:
        log(f"❌ Error al obtener el last_watermark: {str(e)}", level="error")
        return None
    

def get_current_watermark(engine, table_name,  watermark_column, watermark_type):
    try:
        # Obtener el actual watermark
        current_watermark_query = f"""
        SELECT  MAX({watermark_column}) AS current_watermark
        FROM {table_name}
        """         

        response = fetch_data(engine, current_watermark_query)
       
        current_watermark = None
        if response["success"]:
            current_watermark_df = response["data"]               
            # Manejar el current_watermark
            if current_watermark_df is not None and not current_watermark_df.empty and current_watermark_df.iloc[0, 0] is not None:
                current_watermark = current_watermark_df.iloc[0, 0]
            else:
                if watermark_type.upper() == "DATETIME":
                    current_watermark = pd.to_datetime('1990-01-01 00:00:00')  # Valor por defecto para fechas                    
                elif watermark_type.upper() == "INT":
                    current_watermark = 0  # Valor por defecto para enteros
        else:            
            log(f"❌ Error al obtener datos: {response['error']}", level="error")    
            if watermark_type.upper() == "DATETIME":
                current_watermark = pd.to_datetime('1990-01-01 00:00:00')  # Valor por defecto para fechas                    
            elif watermark_type.upper() == "INT":
                current_watermark = 0  # Valor por defecto para enteros
            else:
                raise ValueError(f"❌ Tipo de watermark no soportado: {watermark_type}")


        # Formatear current_watermark a formato datetime compatible con sql
        if watermark_type.upper() == "DATETIME":
            now = pd.Timestamp.now()
            if current_watermark > now:
                log("Fecha y hora mayor al momento de ejecución", level="warning")
                current_watermark = format_datetime_for_sqlserver(now)
            else:
                current_watermark = format_datetime_for_sqlserver(current_watermark)
     
        return current_watermark
    except Exception as e:
        log(f"❌ Error al obtener el current_watermark: {str(e)}", level="error")
        return None
    

    # Funcion para validar las columnas de tipos compatibles

def get_valid_columns(_engine, _table_name, _schema='dbo'):
    query = f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{_table_name}' AND TABLE_SCHEMA = '{_schema}'
    """
    df = pd.read_sql(query, _engine)
    valid_columns = df[~df['DATA_TYPE'].isin(['hierarchyid', 'geometry', 'geography', 'sql_variant'])]['COLUMN_NAME']
    return valid_columns.tolist()
     

def get_sql_table_schema(row,  resource): #(engine, schema_name, json_config):
    schema_info = {}
    engine = create_db_connection(row['serverdb'], 1433, row['db'], row['userDB'], row['userDBPwd'])

    for (resource_grouped, project), group in resource.groupby(["resource_name", "project"]):
        log(f"🔹 PLATFORM → {row['id_Partner']} | Recurso {resource_grouped} | Proyecto: {project} | Obteniendo Schema" , level="info")
        query = f"""
             SELECT COLUMN_NAME, DATA_TYPE
             FROM INFORMATION_SCHEMA.COLUMNS
             WHERE TABLE_NAME = '{group['table_name'].iloc[0]}' AND TABLE_SCHEMA = '{row['schem']}'
        """
        df_schema = pd.read_sql(query, engine)
        schema_info[resource_grouped] = list(zip(df_schema['COLUMN_NAME'], df_schema['DATA_TYPE']))
    return schema_info     

def get_sql_table_schema_database(row,  resource): 
    schema_info = {}
    engine = create_db_connection(row['serverdb'], 1433, row['db'], row['userDB'], row['userDBPwd'])

    for (resource_grouped, project), group in resource.groupby(["resource_name", "project"]):
        log(f"🔹 PLATFORM → {row['db']} | Recurso {resource_grouped} | Proyecto: {project} | Obteniendo Schema" , level="info")
        query = f"""
             SELECT COLUMN_NAME, DATA_TYPE
             FROM INFORMATION_SCHEMA.COLUMNS
             WHERE TABLE_NAME = '{group['table_name'].iloc[0]}' AND TABLE_SCHEMA = '{group['schem']}'
        """
        df_schema = pd.read_sql(query, engine)
        schema_info[resource_grouped] = list(zip(df_schema['COLUMN_NAME'], df_schema['DATA_TYPE']))
    return schema_info   

def save_data(df_data, project_name, table_name, df_schema, _process_execution_id, table_path, _conn_mgr_fabric, _process_name, _notebookutils, _write_deltalake):
    try: 
        records_quantity = len(df_data)
        log(f"💾 Recurso {table_name} |  Se guardarán {records_quantity} registros...", level="info")
        df_clean = clean_data(df_data, df_schema[table_name])       

        if "CreatedTS" in df_clean.columns:
            df_clean["CreatedTS"] = pd.to_datetime(df_clean["CreatedTS"], errors="coerce", utc=True)
            df_clean["CreatedTS"] = df_clean["CreatedTS"].fillna(pd.Timestamp("1990-01-01", tz="UTC"))

        if "idPartner" in df_clean.columns:
            df_clean["idPartner"]= pd.to_numeric(df_clean["idPartner"], errors='coerce').fillna(0).astype(np.int64)     

        # Guardar los datos en Delta Lake
        storage_options = {
            "bearer_token": _notebookutils.credentials.getToken("storage"),
            "use_fabric_endpoint": "true"
        }
        _write_deltalake(table_path, df_clean, mode='append', schema_mode='merge', engine='rust', storage_options=storage_options)
       
      
        # Guardar log de recolección de confirmación
        log_operation(_conn_mgr_fabric, project_name, 0, '', '', '',
                                  '', '', _process_name, '', 'Success ',
                                  _process_execution_id, 'UU', '', '', '')

        
        log(f"✅ Guardado exitoso: {records_quantity} registros en {table_name}.", level="info")

        del df_data
        del df_clean
        gc.collect()
        return True  # Indicar éxito


    except Exception as e:
         # Guardar log de recolección de confirmación
        log_operation(_conn_mgr_fabric, project_name, 0, '', '', 
        # '',
                                  '', '', _process_name, f"❌ Error al guardar en Delta Lake: {e}", 'Error ',
                                  _process_execution_id, 'UU', '', '', 'False')   

        log(f"❌ Error al guardar la tabla {table_name} en Delta Lake: {e}", level="error")
        gc.collect()
        return False  # Indicar fallo
    

def fetch_data(engine, query, max_retries=10, wait_seconds=30):
    result = {
        "success": False,
        "data": None,
        "error": None  # 'timeout', 'operational_error', 'sqlalchemy_error', 'unknown'
    }

    attempt = 0

    while attempt < max_retries:
        try:
            df = pd.read_sql(query, engine)
            result["success"] = True
            result["data"] = df
            return result

        except OperationalError as e:
            if "timeout" in str(e).lower():
                result["error"] = "timeout"
                attempt += 1
                print(f"⏳ Timeout en intento {attempt}. Esperando {wait_seconds} segundos antes de reintentar...")
                time.sleep(wait_seconds)
                continue  # intenta de nuevo
            else:
                result["error"] = "operational_error"
                return result

        except SQLAlchemyError as e:
            print(f"❌ SQLAlchemyError al obtener datos: {str(e)}")
            result["error"] = "sqlalchemy_error"

            return result

        except Exception as e:
            result["error"] = "unknown"
            return result

    print("❌ Se alcanzó el número máximo de reintentos por timeout.")
    return result


def fetch_data_pagination(engine, query, page_size=5000, max_retries=10, wait_seconds=30):
    """
    Extrae datos de SQL Server en bloques paginados y retorna un único DataFrame.

    Args:
        engine: Conexión SQLAlchemy.
        query (str): Consulta base (sin ORDER BY, sin OFFSET/FETCH).
        page_size (int): Cantidad de registros por página.
        max_retries (int): Número máximo de reintentos en caso de timeout.
        wait_seconds (int): Tiempo de espera entre reintentos.

    Returns:
        dict con:
            success (bool), data (DataFrame o None), error (str o None)
    """
    result = {"success": False, "data": None, "error": None}
    offset = 0
    dfs = []

    while True:
        attempt = 0
        page_loaded = False

        while attempt < max_retries and not page_loaded:
            try:
                paginated_query = f"""
                    {query}                    
                    OFFSET {offset} ROWS FETCH NEXT {page_size} ROWS ONLY
                """
                df_page = pd.read_sql(paginated_query, engine)

                # Si no trae más filas, fin de la paginación
                if df_page.empty:
                    result["success"] = True
                    if dfs:
                        dfs_valid = [df for df in dfs if not df.empty]
                        if dfs_valid:
                            result["data"] = pd.concat(dfs_valid, ignore_index=True)
                        else:
                            # Todos los dfs estaban vacíos → estructura vacía con columnas del primero
                            result["data"] = pd.DataFrame(columns=dfs[0].columns)
                    else:
                        result["data"] = pd.DataFrame()
                        
                    return result

                dfs.append(df_page)
                offset += page_size
                page_loaded = True

            except OperationalError as e:
                if "timeout" in str(e).lower():
                    result["error"] = "timeout"
                    attempt += 1
                    print(f"⏳ Timeout en intento {attempt} (offset {offset}). Reintentando en {wait_seconds}s...")
                    time.sleep(wait_seconds)
                else:
                    result["error"] = "operational_error"
                    return result

            except SQLAlchemyError as e:
                print(f"❌ SQLAlchemyError al obtener datos (offset {offset}): {str(e)}")
                result["error"] = "sqlalchemy_error"
                return result

            except Exception as e:
                print(f"❌ Error desconocido en offset {offset}: {str(e)}")
                result["error"] = "unknown"
                return result

        if not page_loaded:
            print("❌ Se alcanzó el número máximo de reintentos por timeout.")
            return result


def fetch_all_data(row, resource, _process_execution_id, _conn_mgr_fabric, _process_name, _environment, _log_table, df_watermarks=None):
    """
    Extrae todos los datos de las tablas según los recursos especificados.
    
    Args:
        df_watermarks: DataFrame opcional con los watermarks cacheados para optimizar consultas.
                    Si no se proporciona, se harán consultas individuales.
    """
    conn_source = create_db_connection(row['serverdb'], 1433, row['db'], row['userDB'], row['userDBPwd'])

    grouped_extracted_data = defaultdict(list)
    for (resource_grouped, project), group in resource.groupby(["resource_name", "project"]):
        log(f"🔹 PLATFORM → {row['id_Partner']} | Recurso {resource_grouped} | Proyecto: {project}", level="info")

        id_partner = row['id_Partner']
        table_name_source = group['table_name'].iloc[0]
        is_incremental = bool(group["is_incremental"].iloc[0])  # fuerza a booleano
        last_watermark = None
        current_watermark = None
       
         # Determinar el esquema a utilizar
        schema = 'dbo'  # Valor por defecto
        try:
            if 'schem' in row and pd.notna(row['schem']):
                schema = row['schem']
            elif 'schem' in group.columns and pd.notna(group['schem'].iloc[0]):
                schema = group['schem'].iloc[0]
            
            log(f"Using schema: {schema}", level="info")
        except Exception as e:
            log(f"⚠️ Error al determinar el esquema, usando 'dbo': {str(e)}", level="warning")


        # Obtener columnas validas
        columns = get_valid_columns(conn_source, table_name_source, schema)
        columns_sql = ", ".join(columns)


        if is_incremental:
            # Si es incremental → aplicamos filtro por watermark
            last_watermark = get_last_watermark(project, row['id_Partner'], table_name_source, group['watermark_type'].iloc[0],  _conn_mgr_fabric, _environment, _log_table, df_watermarks)
            current_watermark = get_current_watermark(conn_source, table_name_source, group['watermark_column'].iloc[0], group['watermark_type'].iloc[0])
           
            query = f"""
                SELECT {columns_sql}
                FROM {schema}.{table_name_source}
                WHERE {group['watermark_column'].iloc[0]} > '{last_watermark}'
                AND {group['watermark_column'].iloc[0]} <= '{current_watermark}'
                ORDER BY  {group['watermark_column'].iloc[0]}
            """
        else:
            query = f"""
                SELECT {columns_sql}
                FROM {schema}.{table_name_source}
                ORDER BY 1
            """
                
    
        
        response = fetch_data_pagination(conn_source, query)
        if response["success"]:
                        
            df_extracted_data = response["data"]
            df_extracted_data["idPartner"] = id_partner
            df_extracted_data["source_table"] = table_name_source
            df_extracted_data["CreatedTS"] = pd.Timestamp.utcnow()  

            records_quantity = len(df_extracted_data)
            log(f"✅ PLATFORM → {row['id_Partner']} | {table_name_source} | Se extrajeron {records_quantity} registros de la tabla {table_name_source} | Plataforma  {id_partner}", level="info")

            if df_extracted_data is not None and not df_extracted_data.empty:
                if resource_grouped in grouped_extracted_data:
                    grouped_extracted_data[resource_grouped] = pd.concat([grouped_extracted_data[resource_grouped], df_extracted_data], ignore_index=True)
                else:
                        grouped_extracted_data[resource_grouped] = df_extracted_data



            #  Generar log de recolección de datos  
            status = "empty" if df_extracted_data is None or df_extracted_data.empty else "pending"          
            log_operation(_conn_mgr_fabric, project, id_partner, table_name_source, group['watermark_column'].iloc[0], current_watermark,
                        last_watermark, records_quantity, _process_name, '', status, 
                        _process_execution_id, 'I', _environment, row['db'], is_incremental)     


        else:
            log(f"❌ Error al obtener datos: {response['error']}", level="error")
            raise RuntimeError(f"Error en fetch_data: {response['error']}")

    return grouped_extracted_data


def process_platform_connection(_row, _resource, _process_execution_id, _conn_mgr_fabric, _process_name, _environment, _log_table, _max_retries, _retry_wait, df_watermarks=None):
    """
    Lógica de ingesta para una conexión con reintentos.
    
    Args:
        df_watermarks: DataFrame opcional con los watermarks cacheados para optimizar consultas.
                    Si no se proporciona, se harán consultas individuales.
    """
    start_time = time.time()
    intentos = 0

    while intentos < _max_retries:
        try:
            intentos += 1
            log(f"➡ [{intentos}/{_max_retries}] Iniciando {_row['id_Partner']} {_row['db']} en {_row['serverdb']}")
                            
            return fetch_all_data(_row, _resource, _process_execution_id, _conn_mgr_fabric, _process_name, _environment, _log_table, df_watermarks)
            # return f"OK - {_row['id_Partner']} "          


        except Exception as e:
            log(f"⚠️ Error en intento {intentos} para {_row['db']}: {str(e)}", level="warning")
            if intentos < _max_retries:
                log(f"⏳ Esperando {_retry_wait} segundos antes de reintentar...")
                time.sleep(_retry_wait)
            else:
                log(f"❌ Fallo definitivo en {_row['db']} después de {_max_retries} intentos", level="error")
                return f"ERROR - {_row['db']}: {str(e)}"
            

def procces_project(resource_project, df_pf_TryController, df_block_conns, df_schema, _conn_mgr_fabric, _process_name, _environment, _log_table, _max_retries, _retry_wait, _max_workers, _time_sleep, _notebookutils, _write_deltalake, _gl_process_execution_id):
        from watermark_utils import get_all_last_watermarks
        
        # Obtener todos los watermarks una sola vez al inicio
        df_watermarks = get_all_last_watermarks(_conn_mgr_fabric, _environment, _log_table)
        
        process_number = 0
        for batch_num, df_batch in enumerate(get_batches(df_block_conns, batch_size=_max_workers), start=1):
            log(f"🚀 Procesando batch {batch_num} con {len(df_batch)} plataformas", level="info")
            resultados = []            
            process_number += 1
            # Generar un UUID como id de ejecucción
            
            process_execution_id  = str(_gl_process_execution_id) + '-' + str(process_number)
            grouped_by_table = defaultdict(list)
            with ThreadPoolExecutor(max_workers=_max_workers) as executor:
                futures = [executor.submit(process_platform_connection, row, df_pf_TryController, process_execution_id, _conn_mgr_fabric, _process_name, _environment, _log_table, _max_retries, _retry_wait, df_watermarks) for _, row in df_batch.iterrows()]
                for future in as_completed(futures):
                    try:
                        result = future.result() 
                        if result is None:
                            continue
                        for table_name, df in result.items():
                            if df is not None and not df.empty:
                                grouped_by_table[table_name].append(df)

                        log("✅ Future completado y consolidado", level="info")
                        
                    except Exception as e:
                        log(f"❌ Error en future: {e}", level="error")
                    

         
            # 🔹 Concatenar DataFrames por tabla
            final_data_by_table = {
                table_name: pd.concat(dfs, ignore_index=True)
                for table_name, dfs in grouped_by_table.items()
            }

            log("📊 Resumen de final_data_by_table:", level="info")
            for table_name, df in final_data_by_table.items():
                log(f"  - {table_name}: {len(df)} filas × {len(df.columns)} columnas", level="info")

            
            # Esperar antes de guardar
            log(f"⏳ Esperando antes de guardar los datos en Fabric...", level="info")

            time.sleep(_time_sleep)

            # Guardar por tabla
            for table_name, df in final_data_by_table.items():
                if not df.empty:
                    log(f"✅ Guardando {len(df)} filas en la tabla {table_name} en Fabric...", level="info")                    
                    
                    # path_to = f"LH_Bronze_{resource_project}.{table_name}_partition"
                    # path_to = f"LH_Bonze_Generals.{table_name}_partition"                  
                    path_to = f"abfss://WS_Data_Engineering@onelake.dfs.fabric.microsoft.com/LH_{resource_project}.Lakehouse/Tables/bronze/{table_name}"
                    log(f"Guardando en {path_to}")
                    result_save_data = save_data(df, resource_project, table_name, df_schema, process_execution_id, path_to, _conn_mgr_fabric, _process_name, _notebookutils, _write_deltalake)

