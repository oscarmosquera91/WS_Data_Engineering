from logger_utils import log, set_logging
import sqlalchemy
import pandas as pd
from format_utils import sanitize_for_pandas, pandas_time_to_str, format_datetime_for_sqlserver
import time


# --- Implementación mejorada con gestión de conexiones ---
import time
from datetime import datetime, timedelta
from threading import Lock
from typing import Optional, Dict

class ConnectionPool:
    """Pool de conexiones para mantener y reutilizar conexiones activas."""
    
    def __init__(self, max_connections=5, connection_timeout=300):
        self.connections: Dict[str, Dict] = {}  # {connection_key: {connection, last_used, in_use}}
        self.max_connections = max_connections
        self.connection_timeout = connection_timeout  # en segundos
        self.lock = Lock()
        
    def get_connection(self, connection_key: str, connection_factory) -> Optional[object]:
        """Obtiene una conexión del pool o crea una nueva si es necesario."""
        with self.lock:
            # Limpiar conexiones expiradas
            self._cleanup_expired_connections()
            
            # Verificar si existe una conexión disponible
            if connection_key in self.connections:
                conn_info = self.connections[connection_key]
                if not conn_info['in_use'] and not self._is_expired(conn_info['last_used']):
                    conn_info['in_use'] = True
                    conn_info['last_used'] = time.time()
                    log(f"✅ Reutilizando conexión existente para {connection_key}", level="info")
                    return conn_info['connection']
            
            # Si no hay conexiones disponibles y no excedemos el límite, crear nueva
            if len(self.connections) < self.max_connections:
                connection = connection_factory()
                if connection:
                    self.connections[connection_key] = {
                        'connection': connection,
                        'last_used': time.time(),
                        'in_use': True
                    }
                    log(f"✅ Nueva conexión creada para {connection_key}", level="info")
                    return connection
            
            log(f"❌ No se pudo obtener conexión para {connection_key}. Pool lleno.", level="error")
            return None
    
    def release_connection(self, connection_key: str):
        """Libera una conexión para que pueda ser reutilizada."""
        with self.lock:
            if connection_key in self.connections:
                self.connections[connection_key]['in_use'] = False
                self.connections[connection_key]['last_used'] = time.time()
                log(f"✅ Conexión liberada para {connection_key}", level="info")
    
    def _is_expired(self, last_used: float) -> bool:
        """Verifica si una conexión ha expirado."""
        return (time.time() - last_used) > self.connection_timeout
    
    def _cleanup_expired_connections(self):
        """Elimina las conexiones expiradas del pool."""
        current_time = time.time()
        expired_keys = [
            key for key, info in self.connections.items()
            if self._is_expired(info['last_used'])
        ]
        for key in expired_keys:
            del self.connections[key]
            log(f"🧹 Conexión expirada eliminada para {key}", level="info")

class FabricConnectionManager:
    _instance = None
    _lock = Lock()
    
    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
            return cls._instance
    
    def __init__(self, notebookutils=None, ws_id=None, warehouse_type=None, artifact_name=None):
        if not hasattr(self, 'initialized'):
            self.notebookutils = notebookutils
            self.ws_id = ws_id
            self.warehouse_type = warehouse_type
            self.artifact_name = artifact_name
            self.connection_pool = ConnectionPool(max_connections=5, connection_timeout=300)
            self.stats = {
                'total_requests': 0,
                'successful_connections': 0,
                'failed_connections': 0,
                'connection_reuse': 0
            }
            self.initialized = True
    
    def get_warehouse_connection(self):
        """Obtiene una conexión al warehouse desde el pool."""
        connection_key = f"warehouse_{self.artifact_name}"
        return self.connection_pool.get_connection(
            connection_key,
            lambda: get_db_connection_warehouse(
                self.artifact_name, self.ws_id, self.warehouse_type, self.notebookutils
            )
        )
    
    def get_sql_connection(self):
        """Obtiene una conexión SQL desde el pool."""
        connection_key = f"sql_{self.artifact_name}"
        return self.connection_pool.get_connection(
            connection_key,
            lambda: get_db_connection_fabricsqldatabase2(
                self.artifact_name, self.ws_id, self.notebookutils
            )
        )
    
    def get_sql_connection2(self):
        """Obtiene una conexión SQL2 desde el pool."""
        return self.get_sql_connection()  # Usa el mismo método ya mejorado
    
    def release_connection(self, connection_type: str):
        """Libera una conexión para que pueda ser reutilizada."""
        connection_key = f"{connection_type}_{self.artifact_name}"
        self.connection_pool.release_connection(connection_key)
    
    def get_stats(self):
        """Retorna estadísticas de uso de conexiones."""
        return {
            **self.stats,
            'active_connections': len(self.connection_pool.connections),
            'available_slots': self.connection_pool.max_connections - len(self.connection_pool.connections)
        }
    


def create_db_connection(server, port, database, user, password):
    try:
        conn_str = f"mssql+pyodbc://{user}:{password}@{server}:{port}/{database}?driver=ODBC+Driver+18+for+SQL+Server&ApplicationIntent=ReadOnly"
        engine = sqlalchemy.create_engine(conn_str)
        log(f"✅ Conexion satisfactoria al servidor {server} | db {database}", level="info")
        return engine
    except Exception as e:
        log(f"❌ Error al establecer la conexion de {server} {e}", level="error")
        return None


def get_db_connection_warehouse(_artifact_name, _ws_id, _warehouse_type, _notebookutils):
    try:
        conn = _notebookutils.data.connect_to_artifact("AnalyticsDB-OnOff", _ws_id, _warehouse_type)

        return conn
    except Exception as e:
        log(f"❌ Error al establecer la conexion de {_artifact_name}: {e}", level="error")
        return None
    
def get_db_connection_fabricsqldatabase(_artifact_name, _workspaceId, _notebookutils):
    """Devuelve la conexión a Fabric SQL Database."""

    try:
        conn = _notebookutils.data.connect_to_artifact(
            artifact=_artifact_name, 
            workspace=_workspaceId,
            artifact_type=_notebookutils.dataconnections.sql.DataUtils.FABRICSQLDATABASE
        )
        log(f"✅ Conexion satisfactoria | db {_artifact_name}", level="info")
        return conn
    except Exception as e:
        log(f"❌ Error [{type(e).__name__}] al establecer la conexión de fabricsqldatabase {_artifact_name}: {e}", level="error")
        return None



def get_db_connection_fabricsqldatabase2(_artifact_name, _workspaceId, _notebookutils, retries=3, wait_base=5):
    """
    Establece una conexión a Fabric SQL Database con reintentos exponenciales.
    
    Args:
        _artifact_name: Nombre del artefacto
        _workspaceId: ID del workspace
        _notebookutils: Utilidades del notebook
        retries: Número máximo de reintentos
        wait_base: Tiempo base de espera entre reintentos (se multiplica exponencialmente)
    """
    for attempt in range(retries):
        try:
            # Calcular tiempo de espera exponencial
            wait_time = wait_base * (2 ** attempt)
            
            conn = _notebookutils.data.connect_to_artifact(
                artifact=_artifact_name,
                workspace=_workspaceId,
                artifact_type=_notebookutils.dataconnections.sql.DataUtils.FABRICSQLDATABASE
            )
            
            if conn:
                log(f"✅ Conexión satisfactoria | db {_artifact_name} | intento {attempt + 1}", level="info")
                return conn
            
        except Exception as e:
            error_msg = str(e).lower()
            is_timeout = "timeout" in error_msg
            error_type = "timeout" if is_timeout else "error"
            
            if attempt < retries - 1:
                log(f"⚠️ {error_type.title()} en intento {attempt + 1} para {_artifact_name}. "
                    f"Esperando {wait_time}s antes de reintentar... Error: {str(e)}", level="warning")
                time.sleep(wait_time)
            else:
                log(f"❌ Error al establecer la conexión de {_artifact_name} después de {retries} intentos: {str(e)}", 
                    level="error")
                
    return None

