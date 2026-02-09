#%%
import re
import numpy as np
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
SUPPORTED_EXTENSIONS = {".csv", ".txt", ".s2p"}
MAIN_PATTERN = re.compile(r"""
                          ^(?P<datatype>[^_]+)
                          _(?P<wafer>[^_]+)
                          _(?P<doe>[^_]+)
                          _(?P<cage>[^_]+)
                          _die(?P<die>\d+)
                          _(?P<subdie>\d+)
                          _(?P<temperature>-?\d+)C
                          _\#(?P<repeat>\d+)
                          _(?P<device>[^_]+)
                          _ch_(?P<ch_in>\d+)
                          _(?P<ch_out>\d+)
                          _(?P<power>-?\d+)dBm
                          (?P<rest>.*)
                          \.(?:csv|txt|s2p)$""",
                          re.VERBOSE)

def parse_filename(filename: str) -> Dict[str, Any]:
        name = Path(filename).name
        match = MAIN_PATTERN.match(name)

        if not match:
            raise ValueError(f"檔名格式不符: {filename}")

        result = match.groupdict()
        rest = result.pop("rest")

        result["SMU"] = []
        result["arguments"] = []

        if not rest:
            return result

        tokens = rest.strip("_").split("_")
        i = 0
        ec_i = 1
        arg_i = 1
        pass_SMU = False
        while i < len(tokens):
            token = tokens[i]
            if token == "SMU":
                match = re.match(r"([-+]?\d*\.?\d+)([a-zA-Z%]*)", tokens[i + 3])
                result["SMU"].append({f"ec{ec_i} type": tokens[i + 1],
                                      f"ec{ec_i} channel": tokens[i + 2],
                                      f"ec{ec_i} value": (float(match[1]), match[2])})
                i += 4
                ec_i += 1
                continue
            if i + 2 < len(tokens) and token != "arg" and not pass_SMU:
                match = re.match(r"([-+]?\d*\.?\d+)([a-zA-Z%]*)", tokens[i + 2])
                result["SMU"].append({f"ec{ec_i} type": tokens[i],
                                      f"ec{ec_i} channel": tokens[i + 1],
                                      f"ec{ec_i} value": (float(match[1]), match[2])})
                i += 3
                ec_i += 1
                continue
            if token == "arg":
                match = re.match(r"([-+]?\d*\.?\d+)([a-zA-Z%]*)", tokens[i + 1])
                result["arguments"].append({f"arg{arg_i}": (float(match[1]), match[2])})
                arg_i += 1
                i += 2
                pass_SMU = True
                continue
            if token:
                match = re.match(r"([-+]?\d*\.?\d+)([a-zA-Z%]*)", token)
                result["arguments"].append({f"arg{arg_i}": (float(match[1]), match[2])})
                arg_i += 1
            i += 1

        return result
filename = r'DCIV_PIC9-FPN3_DOE1_MRM158_die1_1_25C_#1_D4_ch_9_9_5dBm_SMU_pn_1_900mV.csv'
parse_filename(filename)
# %%
