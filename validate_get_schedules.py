"""
Validator for the DataFrame returned by get_schedules
This script defines a `validate_schedules_df(df)` function which checks for required
columns and basic types/constraints. It also contains a small self-test that runs
on a synthetic DataFrame to demonstrate expected output.
"""
import pandas as pd
from datetime import datetime
from typing import Any, Callable, Optional, Tuple

REQUIRED_COLUMNS = [
    "tables_source_id",
    "project",
    "source_type",
    "table_name",
    "execution_time",
    "status",
]

OPTIONAL_COLUMNS = [
    "keys_info",
    "partition_info",
    "is_incremental",
    "enabled",
    "platform_name",
    "schem",
]


def validate_schedules_df(df: pd.DataFrame):
    """Validate that df contains the expected payload from get_schedules.

    Checks performed:
    - Required columns presence
    - execution_time parsable as HH:MM(:SS)
    - status column numeric (0/1) or boolean
    - is_incremental if present is boolean-like

    Returns: (bool, list_of_messages)
    """
    msgs = []
    if df is None:
        return False, ["DataFrame is None"]

    if not isinstance(df, pd.DataFrame):
        return False, [f"Expected pandas.DataFrame, got {type(df)}"]

    # 1) Required columns
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        msgs.append(f"MISSING_COLUMNS: {missing}")
        return False, msgs
    else:
        msgs.append("REQUIRED_COLUMNS_PRESENT")

    # 2) execution_time parse
    def _parse_time(s):
        try:
            if pd.isna(s):
                return None
            if isinstance(s, (pd.Timestamp, datetime)):
                return s
            # accept H:M or H:M:S
            fmt = "%H:%M:%S" if str(s).count(":") == 2 else "%H:%M"
            return datetime.strptime(str(s), fmt)
        except Exception:
            return None

    invalid_time = []
    for i, val in df["execution_time"].fillna("").items():
        if _parse_time(val) is None:
            invalid_time.append((i, val))
    if invalid_time:
        msgs.append(f"INVALID_EXECUTION_TIME_COUNT: {len(invalid_time)} sample: {invalid_time[:3]}")
        return False, msgs
    else:
        msgs.append("EXECUTION_TIME_OK")

    # 3) status valid
    status_vals = df["status"].dropna().unique().tolist()
    ok_status = all(str(v) in ["0", "1", "True", "False", "true", "false", "0.0", "1.0"] for v in status_vals)
    if not ok_status:
        msgs.append(f"STATUS_HAS_UNEXPECTED_VALUES: {status_vals}")
        return False, msgs
    else:
        msgs.append("STATUS_OK")

    # 4) is_incremental if present
    if "is_incremental" in df.columns:
        vals = df["is_incremental"].dropna().unique().tolist()
        ok_inc = all(str(v).lower() in ["1", "0", "true", "false", "True", "False"] for v in vals)
        if not ok_inc:
            msgs.append(f"IS_INCREMENTAL_UNEXPECTED_VALUES: {vals}")
            return False, msgs
        else:
            msgs.append("IS_INCREMENTAL_OK")

    msgs.append("VALIDATION_PASSED")
    return True, msgs


def _validate_keys_info(value: Any) -> Tuple[bool, str]:
    """Validate keys_info structure: list of dicts with field_name and is_primary_key."""
    if value is None:
        return True, "KEYS_INFO_EMPTY"
    if not isinstance(value, (list, tuple)):
        return False, "KEYS_INFO_NOT_LIST"
    for i, item in enumerate(value):
        if not isinstance(item, dict):
            return False, f"KEYS_INFO_ITEM_{i}_NOT_DICT"
        if "field_name" not in item or "is_primary_key" not in item:
            return False, f"KEYS_INFO_ITEM_{i}_MISSING_KEYS"
    return True, "KEYS_INFO_OK"


def _validate_partition_info(value: Any) -> Tuple[bool, str]:
    """Validate partition_info structure: list of dicts with field_name and type_field."""
    if value is None:
        return True, "PARTITION_INFO_EMPTY"
    if not isinstance(value, (list, tuple)):
        return False, "PARTITION_INFO_NOT_LIST"
    for i, item in enumerate(value):
        if not isinstance(item, dict):
            return False, f"PARTITION_INFO_ITEM_{i}_NOT_DICT"
        if "field_name" not in item or "type_field" not in item:
            return False, f"PARTITION_INFO_ITEM_{i}_MISSING_KEYS"
    return True, "PARTITION_INFO_OK"


def validate_schedules_df_strict(df: pd.DataFrame) -> Tuple[bool, list]:
    """Run the base validation plus structural checks for keys_info and partition_info.

    Returns: (bool, messages)
    """
    ok, msgs = validate_schedules_df(df)
    if not ok:
        return ok, msgs

    # structural checks per-row
    for idx, row in df.iterrows():
        # keys_info
        if "keys_info" in df.columns:
            v = row.get("keys_info")
            k_ok, k_msg = _validate_keys_info(v)
            msgs.append(f"ROW_{idx}_KEYS_INFO: {k_msg}")
            if not k_ok:
                return False, msgs

        # partition_info
        if "partition_info" in df.columns:
            v = row.get("partition_info")
            p_ok, p_msg = _validate_partition_info(v)
            msgs.append(f"ROW_{idx}_PARTITION_INFO: {p_msg}")
            if not p_ok:
                return False, msgs

    msgs.append("STRICT_VALIDATION_PASSED")
    return True, msgs


def validate_schedules_from_conn(
    conn: Any,
    get_schedules_func: Optional[Callable[[Any], pd.DataFrame]] = None,
    allow_import: bool = False,
    strict: bool = False,
) -> Tuple[bool, list, Optional[pd.DataFrame]]:
    """Validate schedules payload obtained from a connection.

    Parameters:
    - conn: DB connection / engine accepted by `get_schedules`.
    - get_schedules_func: callable(conn) -> DataFrame. If None and allow_import=True,
      the function will attempt to import `get_schedules` from `ingestion_utils`.
    - allow_import: if True, attempt a local import of `ingestion_utils.get_schedules` when
      `get_schedules_func` is not provided. (This may import heavy modules.)
    - strict: run the stricter validation that checks `keys_info` / `partition_info` structures.

    Returns: (ok: bool, messages: list[str], df: DataFrame or None)
    """
    msgs = []
    try:
        if get_schedules_func is None:
            if not allow_import:
                msgs.append("NO_GET_SCHEDULES_FUNC_PROVIDED")
                return False, msgs, None
            try:
                from ingestion_utils import get_schedules as _gs  # local import

                get_schedules_func = _gs
            except Exception as e:
                msgs.append(f"IMPORT_GET_SCHEDULES_FAILED: {e}")
                return False, msgs, None

        df = get_schedules_func(conn)
        if df is None:
            msgs.append("GET_SCHEDULES_RETURNED_NONE")
            return False, msgs, None

        if strict:
            ok, m = validate_schedules_df_strict(df)
        else:
            ok, m = validate_schedules_df(df)

        msgs.extend(m)
        return ok, msgs, df

    except Exception as e:
        msgs.append(f"VALIDATION_ERROR: {e}")
        return False, msgs, None


# Self-test with a synthetic DataFrame
if __name__ == "__main__":
    sample = pd.DataFrame([
        {
            "tables_source_id": 1,
            "project": "TiendasOn",
            "source_type": "platform",
            "table_name": "DevicesRoutesTokenSession",
            "execution_time": "00:00",
            "status": 1,
            "keys_info": [],
            "partition_info": [],
            "is_incremental": 1,
        },
        {
            "tables_source_id": 2,
            "project": "TryController",
            "source_type": "database",
            "table_name": "Loans",
            "execution_time": "11:00",
            "status": 1,
            "is_incremental": 0,
        },
    ])

    ok, messages = validate_schedules_df(sample)
    print("VALIDATION_OK:", ok)
    for m in messages:
        print(" -", m)
