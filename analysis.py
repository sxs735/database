import csv
import numpy as np

def tofloat(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return np.nan

def read_spectrum(path,start_idx=None,end_idx=None):
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        head = {}
        data = []
        for i,row in enumerate(reader):
            if start_idx is None and ('=== Min' in row or '=== Average IL (TLS 0) ===' in row):
                start_idx = i
            elif end_idx is None and '=== Mueller Row 1 (TLS 0) ==='in row:
                break
            elif start_idx is not None:
                if i> start_idx:
                    data += [[tofloat(value) for value in row]]
            elif ('WavelengthStart' in row 
                  or 'WavelengthStop' in row 
                  or 'WavelengthStep' in row 
                  or 'SweepRate' in row):
                head[row[0]] = (row[1], row[2])

        if start_idx is not None:
            data = np.array(data, dtype=float)
            data[:,0] *= 1E9
        else:
            raise ValueError("Don't support this file format")
    return head, data