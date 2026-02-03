#%%
import re
import csv
import json
import shutil
from pathlib import Path
from typing import Dict, Optional, Tuple
from database_api import DatabaseAPI
from datetime import datetime

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
    rest = result.pop("rest")

    result["SMU"] = []
    result["arguments"] = set()

    if not rest:
        return result

    tokens = rest.strip("_").split("_")
    i = 0
    pass_SMU = False
    while i < len(tokens):
        token = tokens[i]
        # ---------- SMU ----------
        if token == "SMU":
            result["SMU"].append({"device": tokens[i + 1],
                                         "channel": tokens[i + 2],
                                         "value": tokens[i + 3]})
            i += 4
            continue
        # ---------- SMU (device, ch, value) ----------
        if i + 2 < len(tokens) and token != "arg" and not pass_SMU:
            result["SMU"].append({"device": tokens[i],
                                         "channel": tokens[i + 1],
                                         "value": tokens[i + 2]})
            i += 3
            continue
        # ---------- explicit argument ----------
        if token == "arg":
            result["arguments"].add(tokens[i + 1])
            i += 2
            pass_SMU = True
            continue
        result["arguments"].add(token)
        i += 1
    return result

def move_measurement_data(source_dir: str, target_root: str):
    """
    將原始量測資料搬到 MeasurementData 結構資料夾。
    
    Parameters:
    - source_dir: 原始資料夾路徑，例如 ".\\20260204"
    - target_root: 新資料夾根目錄，例如 ".\\MeasurementData"
    """
    source_dir = Path(source_dir)
    target_root = Path(target_root)
    
    # 記錄每個 DUT 的 repeat 次數
    
    for f in source_dir.iterdir():
        if f.suffix.lower() not in {".csv", ".json"}:
            continue
        
        # 解析檔名
        try:
            meta = parse_filename(f.name)
        except ValueError as e:
            print(f"Skipped {f.name}: {e}")
            continue
        
        # DUT 唯一標識
        dut_key = (meta["wafer"], meta["doe"], meta["die"], meta["cage"], meta["device"])
        repeat = int(meta["repeat"])
        
        # session_name = 原資料夾名稱 + repeat (兩位數)
        session_name = f"{source_dir.name}_rep{repeat:02d}"
        
        # 目標資料夾
        target_dir = target_root / meta["wafer"] / meta["doe"] / f"die{meta['die']}" / meta["cage"] / meta["device"] / session_name
        target_dir.mkdir(parents=True, exist_ok=True)
        
        # 移動檔案
        dst = target_dir / f.name
        shutil.move(str(f), dst)
        print(f"Moved {f.name} -> {dst}")
    
#%%

def batch_import_from_folder(folder_path: str, db_path: str = "spectrum_data.db"):
    """
    批次掃描資料夾並匯入所有 CSV 檔案到 SQLite
    
    Args:
        folder_path: 資料夾路徑
        db_path: SQLite 資料庫路徑
    """
    folder = Path(folder_path)
    
    if not folder.exists():
        print(f"資料夾不存在: {folder_path}")
        return
    
    # 列出所有支援的檔案類型
    data_files = []
    for pattern in ["*.csv", "*.txt", "*.s2p"]:
        data_files.extend(folder.glob(pattern))
    print(f"\n找到 {len(data_files)} 個資料檔案\n")
    
    if not data_files:
        print("沒有找到資料檔案")
        return
    
    # 連接資料庫
    with DatabaseAPI(db_path) as db:
        # 創建資料庫（如果不存在）
        try:
            db.create_database("schema.sql")
        except:
            pass  # 資料庫已存在
        
        successful = 0
        failed = 0
        
        for data_file in data_files:
            print(f"處理: {data_file.name}")
            
            # 解析檔名
            file_info = parse_filename(data_file.name)
            if not file_info:
                failed += 1
                continue
            
            # 檢查或創建 DUT
            dut_id = db.insert_dut(wafer=file_info['wafer'],
                                   doe=file_info['doe'],
                                   die=file_info['die'],
                                   cage=file_info['cage'],
                                   device=file_info['device'])
            
            # 創建測量會話
            session_id = db.insert_measurement_session(dut_id=dut_id,
                                                       session_name=folder.name,
                                                       operator="T&P",
                                                       system_version="CM300v1.0",
                                                       notes='')
            
            # 插入實驗條件
            db.insert_experimental_conditions(session_id, 
                                              {'temperature': (file_info['temperature'], '°C')})
            
            # 插入測量數據記錄
            data_id = db.insert_measurement_data(session_id=session_id,
                                                 data_type=file_info['datatype'],
                                                 file_path=str(data_file))
            
            # 插入測量數據資訊（通道資訊）
            db.insert_data_info(data_id, 
                                {'channel_in': file_info['channel_in'],
                                 'channel_out': file_info['channel_out'],
                                 '?smu': json.dumps(file_info.get('electrical', []), ensure_ascii=False),
                                 '?arg': json.dumps(sorted(file_info.get('arguments', [])), ensure_ascii=False)})
            
            print(f"  ✓ DUT_id: {dut_id}, Session_id: {session_id}\n")
            successful += 1
        
        # 顯示統計信息
        print("=" * 50)
        print(f"匯入完成")
        print(f"成功: {successful}/{len(data_files)}")
        print(f"失敗: {failed}/{len(data_files)}")
        
        # 顯示資料庫統計
        stats = db.get_database_stats()
        print(f"\n資料庫統計:")
        for table, count in stats.items():
            print(f"  {table}: {count}")


def main():
    """主程序"""
    import sys
    
    if len(sys.argv) < 2:
        print("用法: python batch_import.py <資料夾路徑> [資料庫路徑]")
        print("\n範例:")
        print("  python batch_import.py ./data")
        print("  python batch_import.py ./data spectrum_data.db")
        return
    
    folder_path = sys.argv[1]
    db_path = sys.argv[2] if len(sys.argv) > 2 else "spectrum_data.db"
    
    batch_import_from_folder(folder_path, db_path)


if __name__ == "__main__":
    main()
