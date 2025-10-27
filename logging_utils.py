from logger_utils import log, set_logging

# Función para ejecutar el procedimiento almacenado IngestaLogOperation
def log_operation(_conn_mgr_fabric, project_name, id_partner, table_name, watermark_column, current_watermark,
                                  last_watermark, records_quantity, pipeline_name, error, status,
                                  activity_id, op, environment, db, _is_incremental):
    try:
        with _conn_mgr_fabric.get_sql_connection().cursor() as log_cursor:
            # cursor = conn.cursor()
            log_cursor.execute("""
                DECLARE @RC INT;
                EXECUTE @RC = [DataOn].[IngestaLogOperation] ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?;
                SELECT @RC AS ReturnCode;
            """,
        project_name, id_partner, table_name, watermark_column, str(current_watermark), str(last_watermark),
        records_quantity, pipeline_name, error, status, activity_id, op, environment, db, _is_incremental)
        while log_cursor.nextset():
            pass
        log_cursor.commit()
        # cursor.commit()
        return True

    except Exception as e:
        log(f"❌ Error al registrar la operacion en IngestaLogOperation: {e}", level="error")
        return None
    
