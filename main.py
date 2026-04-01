#%%
from pathlib import Path
from database_api import DatabaseAPI
from analysis import *
from tqdm import tqdm

db_path = Path(r"D:\Data\1_DataBase") / "DataBase.db"
#db_path = Path(r"R:\KF處理\SQLite") / "DataBase.db"
#db_path = Path(r"C:\Users\mg942\Desktop\元澄\資料庫") / "DataBase 3.db"

#%%
folder = '260330_R01'
folder_path = Path(r"D:\processing\資料庫整理") / folder
#folder_path = Path(r"C:\Users\mg942\Desktop\元澄") / folder
# Optional batch import step for freshly measured folders
with DatabaseAPI(db_path) as db:
    db.backup_database()
    print(f'Importing folder: {folder}')
    db.import_from_measurement_folder(folder_path,schema_file="schema.sql")
    #db.restore_database(create_backup=False)
#%%
cage = 'R01'
measure_name = '260330_R01'
#%%
print("Starting batch MRM_SPCM analysis...")
print(f"Processing cage: {cage}, measure_name: {measure_name}")
with DatabaseAPI(db_path) as db:
    sessions = db.select_session(measure_name = measure_name, cage = cage)
    for session in tqdm(sessions, desc="Sessions"):
        db.MRM_SPCM_analysis_by_session(session['session_id'],commit=False)
    db.conn.commit()
#%%
print("Starting MRM OMA analysis...")
print(f"Processing cage: {cage}, measure_name: {measure_name}")
with DatabaseAPI(db_path) as db:
    sessions = db.select_session(measure_name = measure_name, cage = cage)
    for session in tqdm(sessions, desc="Sessions"):
        db.MRM_OMA_analysis_by_session(session['session_id'],start=1305, end=1312,commit=False)
    db.conn.commit()

#%%
print("Starting MRM tuning analysis...")
print(f"Processing cage: {cage}, measure_name: {measure_name}")
with DatabaseAPI(db_path) as db:
    sessions = db.select_session(measure_name = measure_name, cage = cage)
    for session in tqdm(sessions, desc="Sessions"):
        db.MRM_tuning_analysis_by_session(session['session_id'],start=1305, end=1315, commit=False)
    db.conn.commit()
#%%
print("Starting MRM SSRF analysis...")
print(f"Processing cage: {cage}, measure_name: {measure_name}")
with DatabaseAPI(db_path) as db:
    sessions = db.select_session(measure_name = measure_name, cage = cage)
    for session in tqdm(sessions, desc="Sessions"):
        db.MRM_SSRF_analysis_by_session(session['session_id'],commit=False)
    db.conn.commit()

#%%
print("Starting Loss analysis...")
print(f"Processing cage: {cage}, measure_name: {measure_name}")
with DatabaseAPI(db_path) as db:
    sessions = db.select_session(measure_name = measure_name, cage = cage)
    for session in tqdm(sessions, desc="Sessions"):
        db.Loss_analysis_by_session(session['session_id'],commit=False)
    db.conn.commit()

#%%
# Export a full database snapshot for sharing/reporting
with DatabaseAPI(db_path) as db:
    
    #output_path = db.export_all_tables_to_xlsx(db_path.parent / "database_export.xlsx")
    measure_ids = db.select_measurements(measure_name = '260330_die32_R01')
    for measure in measure_ids:
        #data_ids = db._select_data_by_measure_id(measure['measure_id'])
        db.delete_record('Measurement', measure['measure_id'])
    db.remove_empty_dirs()
    
# %%
folder = Path(r"D:\processing\資料庫整理\1.處理中\250927\exchange")
files = [f for f in folder.iterdir() if f.is_file()]
for file_path in files:
    try:
        exchange_2ports(file_path)
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
# %%
