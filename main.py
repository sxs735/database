#%%
from pathlib import Path
from database_api import DatabaseAPI
from analysis import *
from tqdm import tqdm

# folder_list = ['260122_3',
#                '260122_4',
#                '260122_manual',
#                '260123',
#                '260127',
#                '260127_swr0.5nm',
#                '260127_swr1nm',
#                '260127_swr10nm',
#                '260129',
#                '260204_cage158_D4_repeat20',
#                '260205_cage158_D4_5die',
#                '260205_cage158_D4_die51',
#                '260205_mapping',
#                '260206_mapping',
#                '260212_mapping']
db_path = Path(r"C:\Users\mg942\Desktop\元澄\資料庫") / "DataBase.db"
folder_list = ['20260202']
#%%
# Optional batch import step for freshly measured folders
for folder_i in folder_list[:]:
    print(folder_i)
    folder_path = Path(f"C:\\Users\\mg942\\Desktop\\元澄\\PIC9-FPN3_DOE1_MRM033_DC&RF_3dB\\{folder_i}")
    with DatabaseAPI(db_path) as db:
        db.import_from_measurement_folder(folder_path,schema_file="schema.sql")
#%%
# Build the cage/measure_name combinations to analyze, skipping cage2
with DatabaseAPI(db_path) as db:  
    cmd = '''SELECT DISTINCT d.cage,m.measure_name
             FROM DUT d
             JOIN Measurement m ON d.DUT_id = m.DUT_id
             WHERE d.cage <> 'cage2'
             ORDER BY d.cage, m.measure_name;'''
    res = [(res['cage'], res['measure_name']) for res in db.query(cmd)]

#%%     Run the MRM_SPCM analysis for every relevant session
for cage, measure_name in res[:]:
    print(f"Processing cage: {cage}, measure_name: {measure_name}")
    with DatabaseAPI(db_path) as db:
        session_ids = db.select_session_ids_by_measure_name_and_dut(measure_name = measure_name, cage = cage)
        for session_id in tqdm(session_ids, desc="Sessions"):
           db.MRM_SPCM_analysis_by_session(session_id,commit=False)
        db.conn.commit()
#%%     Run the MRM_OMA analysis for every relevant session
for cage, measure_name in res[:]:
    print(f"Processing cage: {cage}, measure_name: {measure_name}")
    with DatabaseAPI(db_path) as db:
        session_ids = db.select_session_ids_by_measure_name_and_dut(measure_name = measure_name, cage = cage)
        for session_id in tqdm(session_ids, desc="Sessions"):
            db.MRM_OMA_analysis_by_session(session_id,commit=False)
        db.conn.commit()





#%%  
# Alternate query example: exclude WDM devices to inspect combinations quickly
with DatabaseAPI(db_path) as db:  
    cmd = '''SELECT DISTINCT d.cage,m.measure_name
             FROM DUT d
             JOIN Measurement m ON d.DUT_id = m.DUT_id
             WHERE d.device <> 'WDM'
             ORDER BY d.cage, m.measure_name;'''
    a = db.query(cmd)
    # cursor = db.conn.execute("""
    # SELECT sql FROM sqlite_master
    # WHERE type='table' AND name='FeatureMetrics'
    # """)
    # print(cursor.fetchone()[0])
    

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
cage='cage1'
measure_name='20260202'
# Quick-look plot to visually inspect the first SPCM trace in a session
with DatabaseAPI(db_path) as db:
    non_info = db.select_rawdata_by_data_id(1809)
    max_info = db.select_rawdata_by_data_id(1807)
    filepath_v0 = Path(db_path).parent / non_info['file_path']
    filepath_vh = Path(db_path).parent / max_info['file_path']
    head, data_v0 = read_spectrum_lite(filepath_v0)
    head, data_vh = read_spectrum_lite(filepath_vh)
    x = data_v0[:, 0]
    col = 3 if data_v0.shape[1] == 5 else 2
    y0 = data_v0[:, col] - data_v0[:, 1]
    yh = data_vh[:, col] - data_vh[:, 1]
    res,_,_ = MRM_OMA_analysis(data_vh, data_v0, start=1310, end=1315)
    plt.plot(x,y0)
    plt.plot(x,yh)
    plt.show()
# %%
