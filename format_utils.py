import time   # módulo completo → para sleep
import datetime  # módulo completo → para datetime.datetime y datetime.time
import math
import pandas as pd
from logger_utils import log, set_logging 
import numpy as np



def get_current_day():
    """Obtiene el nombre del día actual traducido al castellano."""
    to_day = datetime.datetime.today().strftime("%A")
    day_en = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    day_es = ["Lunes", "Martes", "Miercoles", "Jueves", "Viernes", "Sabado", "Domingo"]
    translated_day = dict(zip(day_en, day_es))
    return translated_day[to_day].upper()


def pandas_time_to_str(df):
    """Convierte todas las columnas datetime.time en formato HH:mm:ss"""
    for col_name in df.columns:
        if pd.api.types.is_object_dtype(df[col_name]):
            if all(isinstance(v, datetime.time) or pd.isna(v) for v in df[col_name]):
                df[col_name] = df[col_name].apply(
                    lambda x: x.strftime("%H:%M:%S") if pd.notna(x) else None
                )
    return df

def sanitize_for_pandas(df):
    """Convierte tipos no soportados o ambiguos para evitar problemas"""
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
            df[col] = df[col].astype(str)       
        if df[col].dtype == "object":
            df[col] = df[col].astype(str)
    return df


def format_datetime_for_sqlserver(value):
    """
    Convierte un datetime, pandas.Timestamp o string ISO en formato compatible con SQL Server:
    'YYYY-MM-DD HH:MM:SS.mmm'
    """
    if value is None:
        return None

    # Si ya es datetime o pandas.Timestamp
    if isinstance(value, (datetime.datetime, pd.Timestamp)):
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # cortar a milisegundos

    # Si es string (ISO u otro formato reconocible por pandas)
    if isinstance(value, str):
        try:
            parsed = pd.to_datetime(value, errors="coerce")
            if pd.isna(parsed):
                raise ValueError(f"❌ No se pudo parsear la fecha: {value}")
            return parsed.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        except Exception as e:
            raise ValueError(f"❌ Error al formatear string '{value}' → {e}")

    raise TypeError(f"❌ Tipo de dato no soportado: {type(value)}")

def clean_data(df, schema):
    import unicodedata

    def _to_str_safe(v):
        """Convertir un valor a str seguro en UTF-8.

        - Si es bytes: decodifica con 'utf-8' usando errors='replace'.
        - Si es str: lo normaliza a NFC.
        - Si es NaN/None: devuelve cadena vacía.
        """
        if pd.isna(v):
            return ""
        try:
            if isinstance(v, (bytes, bytearray)):
                s = v.decode('utf-8', errors='replace')
            else:
                s = str(v)
            # Normalizar a forma compuesta (NFC) para consistencia
            return unicodedata.normalize('NFC', s)
        except Exception:
            # En caso extremo, forzar str y reemplazar caracteres inválidos
            return str(v).encode('utf-8', errors='replace').decode('utf-8', errors='replace')
    
    for column, dtype in schema:
        if column not in df.columns:
            continue

        if dtype in ['varchar', 'nvarchar', 'char', 'nchar', 'text']:
            # Aplicar conversión segura a UTF-8 antes de upper
            df[column] = df[column].apply(_to_str_safe)
            df[column] = df[column].str.upper()

        elif dtype in ['int', 'bigint', 'smallint', 'tinyint']:
            df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0).astype(np.int64)        

        elif dtype in ['decimal', 'numeric', 'float', 'real', 'money']:
            df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0.0)

        elif 'date' in dtype or 'time' in dtype:
            df[column] = pd.to_datetime(df[column], errors='coerce', utc=True)
            df[column] = df[column].fillna(pd.Timestamp("1990-01-01", tz="UTC"))

        elif dtype in ['bit', 'boolean']:
            df[column] = df[column].fillna(False).astype(bool)

        else:
            # fallback para tipos no mapeados: tratar como texto seguro
            df[column] = df[column].apply(_to_str_safe)
            df[column] = df[column].str.upper()

    # Validación para columnas adicionales no incluidas en el esquema
    for col in df.columns:
        if col not in [c[0] for c in schema]:
            if df[col].dtype == object:
                df[col] = df[col].apply(_to_str_safe)
            elif pd.api.types.is_integer_dtype(df[col]):
                df[col] = df[col].fillna(0).astype('Int64')
            elif pd.api.types.is_float_dtype(df[col]):
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
            elif pd.api.types.is_bool_dtype(df[col]):
                df[col] = df[col].fillna(False).astype(bool)
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].fillna(pd.Timestamp("1990-01-01"))
            else:
                df[col] = df[col].fillna("").astype(str)

    return df
