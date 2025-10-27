import pandas as pd
from logger_utils import log, set_logging


# Función para obtener todas las conexiones
def get_connections_partner(project, engine):
    try:
        query = """
            SELECT 
                D.serverdb, D.db, D.id, D.id_Partner, D.userDB, D.userDBPwd, D.schem
            FROM 
                adm.DBCONNECTION_PARTNERS AS D
                INNER JOIN adm.PARTNERS P ON P.id = D.id_Partner
                INNER JOIN adm.PlatformParameters AS PP ON PP.idPartner = D.id_Partner
            WHERE 
                PP.ppTypeProject = ? AND D.id_Partner IN (9002)
            ORDER BY D.id
        """
        db_connections = pd.read_sql(query, engine, params=(project,))
        
        if db_connections.empty:            
            log("⚠️ No se encontraron conexiones para las plataformas seleccionadas.", level="warning")
            return None

        # Agregar un índice de fila
        db_connections.reset_index(drop=True, inplace=True)
        db_connections['row'] = db_connections.index + 1

        log(f"✅ Se obtuvieron {len(db_connections)} conexiones.", level="info")
        return db_connections

    except Exception as e:        
        log(f"❌ Error al obtener las conexiones: {e}", level="error")
        return None


def get_connections_db(project, engine, db_list):
    """
    Obtiene las conexiones para un proyecto y una lista específica de bases de datos.
    
    Args:
        project: ID del proyecto
        engine: Conexión a la base de datos
        db_list: Lista de nombres de bases de datos para filtrar
    """
    try:
        # Aclaración: el primer parámetro pasado a la consulta corresponde a
        # PP.ppTypeProject en la cláusula WHERE (es decir, `project` == ppTypeProject).
        # Si la lista de bases de datos está vacía, no tiene sentido construir la
        # consulta con un IN () vacío, así que devolvemos None con un aviso.
        if not db_list:
            log("⚠️ La lista de bases de datos está vacía. Ninguna conexión solicitada.", level="warning")
            return None
        # Construir la consulta con placeholders para la lista de bases de datos
        placeholders = ','.join(['?' for _ in db_list])
        query = f"""
            SELECT DISTINCT
                 D.serverdb
                ,D.db
                ,0 AS id
                ,'Tcadmindb' AS userDB
                ,'V3nt4sC0ntr0l20$' AS userDBPwd
                ,0 as id_Partner
            FROM 
                adm.DBCONNECTION_PARTNERS  AS D
                INNER JOIN adm.PlatformParameters AS P ON P.idPartner = D.id_Partner
            WHERE
                    P.ppTypeProject = ?
                    AND D.db IN ({placeholders})
        """
        # Combinar los parámetros: primero el project y luego la lista de dbs
        params = tuple([project] + list(db_list))
        db_connections = pd.read_sql(query, engine, params=params)
        
        if db_connections.empty:            
            log("⚠️ No se encontraron conexiones para las bases de datos seleccionadas.", level="warning")
            return None

        # Agregar un índice de fila
        db_connections.reset_index(drop=True, inplace=True)
        db_connections['row'] = db_connections.index + 1

        log(f"✅ Se obtuvieron {len(db_connections)} conexiones.", level="info")
        return db_connections

    except Exception as e:        
        log(f"❌ Error al obtener las conexiones: {e}", level="error")
        return None


def get_connections_partner_filter_db(project, engine, db_list):
    """
    Obtiene las conexiones para un proyecto y una lista específica de bases de datos.
    
    Args:
        project: ID del proyecto
        engine: Conexión a la base de datos
        db_list: Lista de nombres de bases de datos para filtrar
    """
    try:
        # Aclaración: el primer parámetro pasado a la consulta corresponde a
        # PP.ppTypeProject en la cláusula WHERE (es decir, `project` == ppTypeProject).
        # Si la lista de bases de datos está vacía, no tiene sentido construir la
        # consulta con un IN () vacío, así que devolvemos None con un aviso.
        if not db_list:
            log("⚠️ La lista de bases de datos está vacía. Ninguna conexión solicitada.", level="warning")
            return None
        # Construir la consulta con placeholders para la lista de bases de datos
        placeholders = ','.join(['?' for _ in db_list])
        query = f"""
            SELECT D.serverdb, D.db, D.id, D.id_Partner, D.userDB, D.userDBPwd, D.schem
            FROM 
                adm.DBCONNECTION_PARTNERS  AS D
                INNER JOIN adm.PlatformParameters AS P ON P.idPartner = D.id_Partner
            WHERE
                    P.ppTypeProject = ?
                    AND D.db IN ({placeholders})
        """
        # Combinar los parámetros: primero el project y luego la lista de dbs
        params = tuple([project] + list(db_list))
        db_connections = pd.read_sql(query, engine, params=params)
        
        if db_connections.empty:            
            log("⚠️ No se encontraron conexiones para las bases de datos seleccionadas.", level="warning")
            return None

        # Agregar un índice de fila
        db_connections.reset_index(drop=True, inplace=True)
        db_connections['row'] = db_connections.index + 1

        log(f"✅ Se obtuvieron {len(db_connections)} conexiones.", level="info")
        return db_connections

    except Exception as e:        
        log(f"❌ Error al obtener las conexiones: {e}", level="error")
        return None