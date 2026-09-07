#%%
from pathlib import Path
from database_api import DatabaseAPI
from analysis import *
from tqdm import tqdm

db_path = Path(r"X:\1_Database") / "DataBase.db"
#local = Path(r"D:\Data\1_DataBase\processing")
local = Path(r"X:\1_Database\processing")

#%%
for folder in ['260624_mCouple_repeat']:
    folder_path = local / folder
    with DatabaseAPI(db_path) as db:
        db.backup_database()
        print(f'Importing folder: {folder}')
        db.import_from_measurement_folder(folder_path,schema_file="schema.sql")
        #db.restore_database(create_backup=False)

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
        db.SSRF_analysis_by_session(session['session_id'],commit=False)
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
for measure_name in ['260731_AMD_cage158_D4']:
    print("Starting SSRF-MTK analysis...")
    print(f"measure_name: {measure_name}")
    with DatabaseAPI(db_path) as db:
        sessions = db.select_session(measure_name = measure_name)#, cage = cage)
        for session in tqdm(sessions, desc="Sessions"):
            db.SSRF_analysis_by_session(session['session_id'],commit=False)
        db.conn.commit()

#%%
with DatabaseAPI(db_path) as db:
    measure_name_list = ['260706_J-FA04134196_AM']
    for measure_name in measure_name_list:
        print(f"Processing measure_name: {measure_name}")
        measure_ids = db.select_measurements(measure_name = measure_name)
        for measure in measure_ids:
            db.delete_record('Measurement', measure['measure_id'])

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
import matplotlib.pyplot as plt
save_folder = Path(r"Y:\量測資料\1_DataBase\Processing\260624_mCouple_repeat")
measure_name = '260624_mCouple_repeat'
reference_name = '260615_mCouple_R01_100pm'
with DatabaseAPI(db_path) as db:
    sessions = db.select_session(measure_name = measure_name)
    sessions_ref = db.select_session(measure_name = reference_name)
    group = {}
    ref = {}
    for session in sessions:
        spcm_info = db.select_rawdata_files(session['session_id'],data_type='SPCM')[0]
        opt_ch = (spcm_info['optical_input_channel'],spcm_info['optical_output_channel'])
        if opt_ch not in group:
            group[opt_ch] = [spcm_info]
        else:
            group[opt_ch].append(spcm_info)

    for session in sessions_ref:
        spcm_info = db.select_rawdata_files(session['session_id'],data_type='SPCM')[0]
        opt_ch = (spcm_info['optical_input_channel'],spcm_info['optical_output_channel'])
        if opt_ch not in ref:
            ref[opt_ch] = [spcm_info]
        else:
            ref[opt_ch].append(spcm_info)
    
    group_tf = {('1','1'): ['Group1','Group6'], 
                ('2','2'): ['Group2','Group7'], 
                ('3','3'): ['Group3','Group8'], 
                ('4','4'): ['Group4','Group9'],
                ('5','5'): ['Group5','Group10'],
                ('6','6'): ['Group11','Group16'],
                ('7','7'): ['Group12','Group17'],
                ('8','8'): ['Group13','Group18'],
                ('9','9'): ['Group14','Group19'],
                ('10','10'): ['Group15','Group20'],
                ('10','11'): ['Group15','Group21']}
    for opt_ch in list(group.keys())[:]:
        repeat = []
        idx = []
        path_ref = Path(db.db_path).parent / ref[opt_ch][0]['file_path']
        _,spcm_ref = read_spectrum_lite(path_ref)
        for spcm_info in group[opt_ch]:
            path = Path(db.db_path).parent / spcm_info['file_path']
            _,mueller_data = read_spectrum_lite(path)
            m = re.search(r'#(\d+)', spcm_info['file_path'])
            ind = int(m.group(1))
            idx.append(ind)
            mueller_data = mueller_data[:,[0,1,3]]
            repeat.append(mueller_data)#-spcm_ref[:,[0,1,3]])
            

        repeat = np.array(repeat)
        wavelenght = mueller_data[:,0]
        average = np.nanmean(repeat, axis=0)
        diff = repeat - average

        fig1, (ax11, ax12) = plt.subplots(1, 2, figsize=(14, 5), sharex=True)
        variation1 = []
        variation2 = []
        for ind,i in zip(idx,range(repeat.shape[0])):
            ax11.plot(wavelenght, -diff[i,:,1], label=f"#{ind}")
            variation1.append(np.max(diff[i,:,1]))
            variation1.append(np.min(diff[i,:,1]))
            ax12.plot(wavelenght, -diff[i,:,2], label=f"#{ind}")
            variation2.append(np.max(diff[i,:,2]))
            variation2.append(np.min(diff[i,:,2]))
        ax11.set_title(f"{group_tf[opt_ch][0]}, Variation: {(np.max(variation1)-np.min(variation1)):.3f}dB")
        ax11.set_xlabel("Wavelength (nm)")
        ax11.set_ylabel("Loss (dB)")
        ax11.legend()
        ax12.set_title(f"{group_tf[opt_ch][1]}, Variation: {(np.max(variation2)-np.min(variation2)):.3f}dB")
        ax12.set_xlabel("Wavelength (nm)")
        ax12.set_ylabel("Loss (dB)")
        ax12.tick_params(axis='y', labelleft=True)
        ax11.set_ylim(-0.1, 0.1)
        ax12.set_ylim(-0.1, 0.1)
        ax12.legend()

        fig1.tight_layout()
        plt.show()
        save_path = save_folder / f"{group_tf[opt_ch][0]}_{group_tf[opt_ch][1]}_repeat.png"
        if not save_path.parent.exists():
            save_path.parent.mkdir(parents=True)
        fig1.savefig(save_path)





