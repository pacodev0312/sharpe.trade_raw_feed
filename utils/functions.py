import numpy as np
from datetime import datetime as dt

def convert_nan_int(value, field=""):
    try:
        if value is None:
            return None
        if isinstance(value, int):
            return value
        value = float(value)
        if not np.isnan(value):
            return int(value)
        else:
            return None
    except Exception as ex:
        print(f"utils/functions/convert_nan_int {field}: {ex}")
        return None

def convert_nan_float(value, field=""):
    try:
        pass
    except Exception as ex:
        print(f"utils/functions/calc_nan_float Error {field}: {ex}")

def convert_nan_string(value, field=""):
    try:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        if not np.isnan(value):
            return str(value)
        else:
            return None
    except Exception as ex:
        print(f"utils/functions/convert_nan_string {field}: {ex}")
        return None
    
def convert_nan_date(value, field=""):
    try:
        expiry = convert_nan_string(value, field)
        if expiry is None:
            return None
        return dt.strptime(expiry, "%d-%m-%Y")
    except Exception as ex:
        print(f"utils/functions/convert_nan-date Error {field}: {ex}")
        return None
    
def convert_nan_ticksize(value, field=""):
    try:
        result = []
        v = convert_nan_string(value, field)
        if v is not None:
            arr = v.split("-")
            for arg in arr:
                result.append(float(arg))
            return result
        else:
            return None
    except Exception as ex:
        print(f"utils/functions/convert_nan_ticksize Error {field}: {ex}")
        return None