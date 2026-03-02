#%%
from pathlib import Path
from database_api import DatabaseAPI
from analysis import *
from tqdm import tqdm

db_path = Path(r"D:\Data\1_DataBase") / "DataBase.db"

#%%
folder = '260129'
# Optional batch import step for freshly measured folders
with DatabaseAPI(db_path) as db:
    db.backup_database()
    print(f'Importing folder: {folder}')
    folder_path = Path(f"D:\\processing\\資料庫整理\\{folder}")
    db.import_from_measurement_folder(folder_path,schema_file="schema.sql")
    #db.restore_database(create_backup=False)
#%%
cage = 'cage158'
measure_name = '260122_3'
#%%
print("Starting batch MRM_SPCM analysis...")
print(f"Processing cage: {cage}, measure_name: {measure_name}")
with DatabaseAPI(db_path) as db:
    session_ids = db.select_session_ids_by_measure_name_and_dut(measure_name = measure_name, cage = cage)
    for session_id in tqdm(session_ids, desc="Sessions"):
        db.MRM_SPCM_analysis_by_session(session_id,commit=False)
    db.conn.commit()
#%%
print("Starting MRM OMA analysis...")
print(f"Processing cage: {cage}, measure_name: {measure_name}")
with DatabaseAPI(db_path) as db:
    session_ids = db.select_session_ids_by_measure_name_and_dut(measure_name = measure_name, cage = cage)
    for session_id in tqdm(session_ids, desc="Sessions"):
        db.MRM_OMA_analysis_by_session(session_id,commit=False)
    db.conn.commit()

#%%
print("Starting MRM tuning analysis...")
print(f"Processing cage: {cage}, measure_name: {measure_name}")
with DatabaseAPI(db_path) as db:
    session_ids = db.select_session_ids_by_measure_name_and_dut(measure_name = measure_name, cage = cage)
    for session_id in tqdm(session_ids, desc="Sessions"):
        db.MRM_tuning_analysis_by_session(session_id, commit=False)
    db.conn.commit()
#%%
print("Starting MRM SSRF analysis...")
print(f"Processing cage: {cage}, measure_name: {measure_name}")
with DatabaseAPI(db_path) as db:
    session_ids = db.select_session_ids_by_measure_name_and_dut(measure_name = measure_name, cage = cage)
    for session_id in tqdm(session_ids, desc="Sessions"):
        db.MRM_SSRF_analysis_by_session(session_id,commit=False)
    db.conn.commit()

#%%  
# Alternate query example: exclude WDM devices to inspect combinations quickly
with DatabaseAPI(db_path) as db:  
    cmd = '''SELECT DISTINCT d.cage,m.measure_name
             FROM DUT d
             JOIN Measurement m ON d.DUT_id = m.DUT_id
             WHERE d.device <> 'WDM'
             ORDER BY d.cage, m.measure_name;'''
    output = db.query(cmd)
#%%
# Export a full database snapshot for sharing/reporting
with DatabaseAPI(db_path) as db:
    output_path = db.export_all_tables_to_xlsx(db_path.parent / "database_export.xlsx")   
# %%
# Manual transaction control helper (when a previous block fails mid-way)
with DatabaseAPI(db_path) as db:
    db.conn.rollback()
# %%
import sqlite3

# Quick integrity check to ensure the SQLite file is healthy
conn = sqlite3.connect(db_path)
print(conn.execute("PRAGMA integrity_check;").fetchone())
# %%
#%matplotlib qt
import matplotlib.pyplot as plt
cage='cage18'
measure_name='260129'
# Quick-look plot to visually inspect the first SPCM trace in a session
with DatabaseAPI(db_path) as db:
    for data_id in [2295,2296,2297,2298,2299,2300]:
        info = db.select_rawdata_by_data_id(data_id)
        filepath = Path(db_path).parent / info['file_path']
        head, data = read_spectrum_lite(filepath)
        x = data[:, 0]
        col = 3 if data.shape[1] == 5 else 2
        y = data[:, col] - data[:, 1]
        #res,_,_ = MRM_OMA_analysis(data_vh, data_v0, start=1310, end=1315)
        plt.plot(x,y)
    plt.show()
# %%
