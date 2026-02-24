#%%
from pathlib import Path
from database_api import DatabaseAPI
from analysis import *
from tqdm import tqdm

folder_path = Path(r"C:\Users\mg942\Desktop\元澄\PIC9-FPN3_DOE1_MRM033_DC&RF_3dB\20260202")
db_path = Path(r"C:\Users\mg942\Desktop\元澄\Data") / "DataBase.db"
#%%
with DatabaseAPI(db_path) as db:
    db.import_from_measurement_folder(folder_path,schema_file="schema.sql")
#%%
with DatabaseAPI(db_path) as db:
    output_path = db.export_all_tables_to_xlsx(db_path.parent / "database_export.xlsx")

#%%
with DatabaseAPI(db_path) as db:
    sql = '''
    SELECT m.measure_id, m.DUT_id, m.measure_name, m.measured_at
    FROM Measurement m
    WHERE NOT EXISTS (
        SELECT 1
        FROM Analyses a
        JOIN MeasureSession ms ON a.session_id = ms.session_id
        WHERE ms.measure_id = m.measure_id)
    ORDER BY m.measured_at'''
    result = db.query(sql)
    measure_id_list = [d['measure_id'] for d in result]
    session_measurements = []
    total_spcm = 0
    for measure_id in measure_id_list:
        spcm_data = db.select_rawdata_by_session_id(measure_id=measure_id, data_type="SPCM")
        session_measurements.append((measure_id, spcm_data))
        total_spcm += len(spcm_data)

    progress_desc = "Processing SPCM files"
    with tqdm(total=total_spcm, desc=progress_desc, unit="file") as pbar:
        for measure_id, spcm_data in session_measurements:
            for idx, info in enumerate(spcm_data):
                path = info['file_path']
                instance_no = idx
                session_idx = info['session_idx']
                head,data = read_spectrum(path)
                x = data[:, 0]
                y = data[:, 3] - data[:, 1]
                result, algorithm_name, version = MRM_SPCM_analysis(x, y)
                if len(result['valley_wavelength'][0]) <= 6:
                    analysis_id = db.insert_analysis(measure_id = measure_id,
                                                     session_idx = session_idx,
                                                     analysis_type = 'MRM_SPCM_analysis',
                                                     instance_no = instance_no,
                                                     algorithm = algorithm_name,
                                                     version = version,
                                                     commit=False)
                    
                    db.insert_sources(analysis_id, info["data_id"], commit=False)
                    for i in range(len(result['valley_wavelength'][0])):
                        feature_id = db.insert_feature(analysis_id=analysis_id, feature_type='basic parameters', feature_idx=i)
                        db.insert_metrics(feature_id, {
                            'valley wavelength': (result['valley_wavelength'][0][i], 'nm'),
                            'FSR (nm)': (result['FSRnm'][0][i], 'nm'),
                            'FSR (GHz)': (result['FSRGHz'][0][i], 'GHz'),
                            'FWHM (nm)': (result['FWHMnm'][0][i], 'nm'),
                            'FWHM (GHz)': (result['FWHMGHz'][0][i], 'GHz'),
                            'Q factor': (result['Q factor'][0][i], '')
                        })
                pbar.update(1)
    db.conn.commit()
    
#%%  
with DatabaseAPI(db_path) as db:  
    cmd = '''SELECT r.data_id
             FROM RawDataFiles r
             JOIN ElectricInfo e ON e.data_id = r.data_id
             WHERE r.session_id = 1
             AND r.data_type = 'SPCM'
             AND e.set_mode LIKE '%pn%';'''
    a = db.query(cmd)

    for d in a:
        data_id = d['data_id']
        info = db.select_electric_info_by_data_id(data_id)
        print(info)

    
# %%
