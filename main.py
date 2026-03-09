#%%
from pathlib import Path
from database_api import DatabaseAPI
from analysis import *
from tqdm import tqdm

db_path = Path(r"D:\Data\1_DataBase") / "DataBase.db"
#db_path = Path(r"R:\KF處理\SQLite") / "DataBase.db"

#%%
folder = '260305_MTK_MRM'
# Optional batch import step for freshly measured folders
with DatabaseAPI(db_path) as db:
    db.backup_database()
    print(f'Importing folder: {folder}')
    folder_path = Path(f"D:\\processing\\資料庫整理\\{folder}")
    db.import_from_measurement_folder(folder_path,schema_file="schema.sql")
    #db.restore_database(create_backup=False)
#%%
cage = 'cage36'
measure_name = '260305_MTK_MRM'
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
        db.MRM_OMA_analysis_by_session(session_id,start=1305, end=1315,commit=False)
    db.conn.commit()

#%%
print("Starting MRM tuning analysis...")
print(f"Processing cage: {cage}, measure_name: {measure_name}")
with DatabaseAPI(db_path) as db:
    session_ids = db.select_session_ids_by_measure_name_and_dut(measure_name = measure_name, cage = cage)
    for session_id in tqdm(session_ids, desc="Sessions"):
        db.MRM_tuning_analysis_by_session(session_id,start=1305, end=1315, commit=False)
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
# Export a full database snapshot for sharing/reporting
with DatabaseAPI(db_path) as db:
    output_path = db.export_all_tables_to_xlsx(db_path.parent / "database_export.xlsx")   