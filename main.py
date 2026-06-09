#%%
from pathlib import Path
from database_api import DatabaseAPI
from analysis import *
from tqdm import tqdm

db_path = Path(r"X:\1_Database") / "DataBase.db"
#local = Path(r"D:\Data\1_DataBase\processing")
local = Path(r"X:\1_Database\processing")

#%%
for folder in ['260608_mCouple_test']:
    folder_path = local / folder
    with DatabaseAPI(db_path) as db:
        db.backup_database()
        print(f'Importing folder: {folder}')
        db.import_from_measurement_folder(folder_path,schema_file="schema.sql")
        #db.restore_database(create_backup=False)
#%%
cage = 'cage40'
measure_name = '260430_MTK_die35'
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
        db.MRM_OMA_analysis_by_session(session['session_id'],start=1308, end=1315,commit=False)
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
for measure_name in ['260601_AMD_MRM','260601_AMD_MRM_50ohm','260601_AMD_MRM_MPI','260601_MTK_MRM','260601_MTK_MRM_50ohm','260601_MTK_MRM_MPI']:
    print("Starting SSRF-MTK analysis...")
    print(f"measure_name: {measure_name}")
    with DatabaseAPI(db_path) as db:
        sessions = db.select_session(measure_name = measure_name)#, cage = cage)
        for session in tqdm(sessions, desc="Sessions"):
            db.MRM_SSRF_MTK_analysis_by_session(session['session_id'],commit=False)
        db.conn.commit()

#%%
with DatabaseAPI(db_path) as db:
    #res = db.select_measurements()
    #measure_name_list = list(set([measure['measure_name'] for measure in res]))
    measure_name_list = ["260525_MTK_MUX_06"]
    for measure_name in measure_name_list:
        print(f"Processing measure_name: {measure_name}")
        measure_ids = db.select_measurements(measure_name = measure_name)
        for measure in measure_ids:
            db.delete_record('Measurement', measure['measure_id'])
#%%
with DatabaseAPI(db_path) as db:
    db.remove_empty_dirs()
#%%
for measure_name in ['260407_A01_25C','260409_A02_25C','260409_A02_85C','260410_A01_85C',
                     '260416_A01_25C','260417_A02_25C','260417_A05_25C','260420_A05_85C',
                     '260422_A04_25C','260423_A04_85C','260423_A04_85C_DetailSweep','260424_A04_25C_DetailSweep',
                     '260424_A04_25C_DetailSweep_IbiasIthr','260427_A04_85C_DetailSweep_IbiasIthr']:
    #measure_name = '260422_A04_25C'
    with DatabaseAPI(db_path) as db:
        measure_ids = db.select_measurements(measure_name = measure_name)
        for measure in measure_ids:
            db.take_rawdata(measure['measure_id'])
    
# %%
folder = Path(r"D:\processing\資料庫整理\1.處理中\250927\exchange")
files = [f for f in folder.iterdir() if f.is_file()]
for file_path in files:
    try:
        exchange_2ports(file_path)
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
# %%
with DatabaseAPI(db_path) as db:
    sessions = db.select_session(measure_name = '260417_A05_25C_Cage9_11')
    for session in sessions:
        dciv_info = db.select_rawdata_files(session['session_id'], data_type='DCIV')
        for idx, info in enumerate(dciv_info):
            dciv_data = read_dcvi(Path(db.db_path).parent / info['file_path'])
            channel = dciv_data['channel']
            voltage = dciv_data['measured voltage'][0]
            current = dciv_data['measured current'][0]
            resistance = voltage / current if current != 0 else np.inf

            analysis_id = db.insert_analysis(session_id = session['session_id'],
                                            analysis_type = 'resistance',
                                            instance_no = idx,
                                            algorithm = 'resistance',
                                            version = '0.0.0',
                                            commit=False)
            db.insert_sources(analysis_id, info["data_id"], commit=False)
            feature_id = db.insert_feature(analysis_id=analysis_id, feature_type='resistance', feature_idx=0, commit=False)
            db.insert_metrics(feature_id, {'Resistance': (float(round(resistance, 3)), 'Ohm')}, commit=False)
    db.conn.commit()

# %%
with DatabaseAPI(db_path) as db:
    res = db.select_measurements()
    measure_name_list = list(set([measure['measure_name'] for measure in res]))
# %%
