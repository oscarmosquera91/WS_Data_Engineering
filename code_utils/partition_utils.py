from logger_utils import log, set_logging
import time   # módulo completo → para sleep
import datetime  # módulo completo → para datetime.datetime y datetime.time
import math
import pandas as pd


def get_block_number(total_blocks=4):
    """Devuelve el número de bloque actual según la hora."""
    offset_hours = 5
    reference_datetime = datetime.datetime.now() - datetime.timedelta(hours=offset_hours)
    current_hour = reference_datetime.hour
    hours_per_block = 24 // total_blocks
    return current_hour // hours_per_block  # 0, 1, 2, 3

def get_block(df, block_number, total_blocks=4):
    total = len(df)
    if total == 0:
        return df  # DataFrame vacío
    block_size = math.ceil(total / total_blocks)
    # Forzar que el bloque no se pase del total
    block_number = min(block_number, math.ceil(total / block_size) - 1)
    start = block_number * block_size
    end = min(start + block_size, total)
    return df.iloc[start:end]


def get_batches(df, batch_size=50):
    """Genera lotes (sub-dataframes) de tamaño batch_size.
    Si el DataFrame tiene menos registros que batch_size, retorna un solo batch con todos los registros.
    """
    total_rows = len(df)
    if total_rows == 0:
        return []  # No hay registros, retorna lista vacía
    elif total_rows <= batch_size:
        return [df]  # Un solo batch con todos los registros
    else:
        # Divide en batches solo si hay más registros que el batch_size
        batches = []
        for start in range(0, total_rows, batch_size):
            batches.append(df.iloc[start:start + batch_size])
        return batches

def get_block_number_in_window(total_blocks, window_start_hour, window_end_hour, now: datetime.datetime = None):
    """
    Calcula el índice de bloque (0..total_blocks-1) dentro de una franja horaria del día.

    Args:
        total_blocks (int): número de bloques en los que dividir la franja.
        window_start_hour (int): hora de inicio (0-23) de la franja, por ejemplo 0 para 00:00.
        window_end_hour (int): hora de fin (0-23) de la franja, por ejemplo 8 para 08:00.
            Si la franja cruza medianoche (ej. start=22, end=6) se maneja correctamente.
        now (datetime.datetime, optional): hora a evaluar (útil para pruebas). Si es None se usa la hora actual.

    Returns:
        int | None: índice del bloque (0-based) si la hora actual está dentro de la franja, si no devuelve None.

    Ejemplo:
        - `get_block_number_in_window(4, 0, 8)` divide la franja 00:00-08:00 en 4 bloques de 2 horas.
    """
    if total_blocks <= 0:
        raise ValueError("total_blocks debe ser >= 1")

    # Normalizar horas
    start = int(window_start_hour) % 24
    end = int(window_end_hour) % 24

    # Now
    current_dt = now or datetime.datetime.now()
    cur_min = current_dt.hour * 60 + current_dt.minute
    start_min = start * 60
    end_min = end * 60

    # Calcular duración en minutos manejando cruce de medianoche
    if start_min <= end_min:
        window_length = end_min - start_min
        if not (start_min <= cur_min < end_min):
            return None
        current_pos = cur_min - start_min
    else:
        # cruza medianoche
        window_length = (24 * 60 - start_min) + end_min
        if not (cur_min >= start_min or cur_min < end_min):
            return None
        if cur_min >= start_min:
            current_pos = cur_min - start_min
        else:
            current_pos = (24 * 60 - start_min) + cur_min

    if window_length == 0:
        # Interpretamos 0 como ventana completa de 24h
        window_length = 24 * 60

    minutes_per_block = window_length / total_blocks
    block_index = int(current_pos // minutes_per_block)
    if block_index >= total_blocks:
        block_index = total_blocks - 1
    return block_index