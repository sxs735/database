#%%
from analysis import *
from pathlib import Path
import numpy as np
folder = Path(r"C:\Users\kuofenglin\OneDrive - 元澄半導體科技股份有限公司\桌面\Origin_data")
path = folder / r"SPCM_MTK-die2-IDN9N480.00#1_C02_cage1_die35_25C_#1_D1_ch_3_3_0dBm_SMU_htr_2_25.0mA.csv"
data = read_spectrum_all(path)
# %%
import json
import numpy as np


def numpy_to_python(obj):

    if isinstance(obj, np.ndarray):
        return obj.tolist()

    if isinstance(obj, np.integer):
        return int(obj)

    if isinstance(obj, np.floating):
        return float(obj)

    if isinstance(obj, dict):
        return {
            key: numpy_to_python(value)
            for key, value in obj.items()
        }

    if isinstance(obj, (list, tuple)):
        return [
            numpy_to_python(value)
            for value in obj
        ]

    return obj


json_data = numpy_to_python(data)

with open("spectrum.json", "w", encoding="utf-8") as f:
    json.dump(
        json_data,
        f,
        ensure_ascii=False,
        separators=(",", ":")
    )
# %%
