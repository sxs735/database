#%%
import re
import shutil
from pathlib import Path
from typing import Dict, Tuple, List
from database_api import DatabaseAPI
from analysis import *

SUPPORTED_EXTENSIONS = {".csv", ".txt", ".s2p"}

MAIN_PATTERN = re.compile(r"""
                          ^(?P<datatype>[^_]+)
                          _(?P<wafer>[^_]+)
                          _(?P<doe>[^_]+)
                          _die(?P<die>\d+)
                          _(?P<cage>[^_]+)
                          _(?P<device>[^_]+)
                          _(?P<temperature>-?\d+)C
                          _rep(?P<repeat>\d+)
                          _ch_(?P<ch_in>\d+)
                          _(?P<ch_out>\d+)
                          _(?P<power>-?\d+)dBm
                          (?P<rest>.*)
                          \.(?:csv|txt|s2p)$""",re.VERBOSE)

def parse_filename(filename: str) -> dict:
    name = Path(filename).name
    m = MAIN_PATTERN.match(name)

    if not m:
        raise ValueError(f"檔名格式不符: {filename}")

    result = m.groupdict()
    rest = result.pop("rest")   # 取出rest並從result移除

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
        # ---------- SMU ----------
        if token == "SMU":
            match = re.match(r'([-+]?\d*\.?\d+)([a-zA-Z%]*)', tokens[i + 3])
            result["SMU"].append({f"ec{ec_i} type": tokens[i + 1],
                                  f"ec{ec_i} channel": tokens[i + 2],
                                  f"ec{ec_i} value": (float(match[1]),match[2])})
            i += 4
            ec_i += 1
            continue
        # ---------- SMU (device, ch, value) ----------
        if i + 2 < len(tokens) and token != "arg" and not pass_SMU:
            match = re.match(r'([-+]?\d*\.?\d+)([a-zA-Z%]*)', tokens[i + 2])
            result["SMU"].append({f"ec{ec_i} type": tokens[i],
                                  f"ec{ec_i} channel": tokens[i + 1],
                                  f"ec{ec_i} value": (float(match[1]),match[2])})
            i += 3
            ec_i += 1
            continue
        # ---------- explicit argument ----------
        if token == "arg":
            match = re.match(r'([-+]?\d*\.?\d+)([a-zA-Z%]*)', tokens[i + 1])
            result["arguments"].append({f'arg{arg_i}': (float(match[1]),match[2])})
            arg_i += 1
            i += 2
            pass_SMU = True
            continue
        if token:
            match = re.match(r'([-+]?\d*\.?\d+)([a-zA-Z%]*)', token)
            result["arguments"].append({f'arg{arg_i}': (float(match[1]),match[2])})
            arg_i += 1
        i += 1
    
    return result

def parse_folder(folder_path: str) -> Tuple[Dict[Path, dict], List[str]]:
    """
    檢測資料夾內所有檔案名稱是否符合指定格式
    
    Parameters:
    - folder_path: 要檢測的資料夾路徑
    
    Returns:
    - (valid_files, invalid_files) 元組
      - valid_files: {Path物件: 解析後的元數據字典, ...}
      - invalid_files: 不符合格式的檔案名稱列表
    """
    folder = Path(folder_path)
    
    if not folder.exists():
        raise FileNotFoundError(f"資料夾不存在: {folder_path}")
    
    valid_files = {}
    invalid_files = []
    
    # 掃描支援的檔案類型（使用glob提高效率）
    for ext in SUPPORTED_EXTENSIONS:
        for f in folder.glob(f"*{ext}"):
            try:
                meta = parse_filename(f.name)
                valid_files[f] = meta
            except ValueError:
                invalid_files.append(f.name)
    
    return valid_files, invalid_files

folder_path = Path(r'C:\Users\mg942\Desktop\元澄\PIC9-FPN3_DOE1_MRM033_DC&RF_3dB\20260202')
db_path = folder_path.parent / "Measurement.db"
target_root = folder_path.parent / "MeasurementData"
valid_files, invalid_files = parse_folder(folder_path)
with DatabaseAPI(db_path) as db:
    try:
        db.create_database("schema.sql")
    except:
        pass
    
    for filepath, file_info in valid_files.items():

        session_name = folder_path.name + ('_rep'+file_info['repeat'] if file_info['repeat']!='1' else '')
        target_dir = target_root / file_info["wafer"] / file_info["doe"] / f"die{file_info['die']}" / file_info["cage"] / file_info["device"] / session_name
        target_dir.mkdir(parents=True, exist_ok=True)
        dst = target_dir / filepath.name

        dut_id = db.insert_dut(wafer=file_info['wafer'],
                                doe=file_info['doe'],
                                die=file_info['die'],
                                cage=file_info['cage'],
                                device=file_info['device'])

        session_id = db.insert_measurement_session(dut_id=dut_id,
                                                    session_name=session_name,
                                                    operator="T&P",
                                                    system_version="CM300v1.0",
                                                    measurement_datetime=folder_path.stat().st_mtime,
                                                    notes='')

        db.insert_experimental_conditions(session_id, 
                                            {'temperature': (file_info['temperature'], '°C')})

        data_id = db.insert_measurement_data(session_id=session_id,
                                                data_type=file_info['datatype'],
                                                created_time = filepath.stat().st_mtime,
                                                file_path=str(dst.resolve()))

        smu = file_info.pop('SMU')
        smu = {k: v for d in smu for k, v in d.items()}
        arguments = file_info.pop('arguments')
        arguments = {k: v for d in arguments for k, v in d.items()}
        other = {}
        if file_info['datatype'] in ['SPCM']:
            other, _ = read_spectrum(filepath)
            

        db.insert_data_info(data_id, 
                            {'channel_in': file_info['ch_in'],
                                'channel_out': file_info['ch_out'],
                                'power': (file_info['power'], 'dBm')}
                                | smu | arguments | other)
        
        dst = target_dir / filepath.name
        shutil.move(str(filepath), dst)


# %%
