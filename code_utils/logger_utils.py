# logger_utils.py

import logging

# Variables de configuración global (podrían moverse a un config.json más adelante)
LOGGING_ENABLED = True
LOG_LEVEL = logging.INFO

# Crear el logger base
logger = logging.getLogger("FabricPipelineLogger")
logger.propagate = False

if not logger.handlers:  # Evita handlers duplicados
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(LOG_LEVEL)

# Función para emitir logs
def log(msg, level="info"):
    if not LOGGING_ENABLED:
        return
    level = level.lower()
    if level == "debug":
        logger.debug(msg)
    elif level == "warning":
        logger.warning(msg)
    elif level == "error":
        logger.error(msg)
    else:
        logger.info(msg)

# Función para configurar logging dinámicamente
def set_logging(enabled: bool = True, level: str = "INFO"):
    global LOGGING_ENABLED
    LOGGING_ENABLED = enabled
    logger.setLevel(getattr(logging, level.upper()))
    logger.info(f"🔧 Logging {'activado' if enabled else 'desactivado'} con nivel {level.upper()}")
