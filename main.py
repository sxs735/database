#%%
from pathlib import Path
from database_api import DatabaseAPI
import numpy as np
from scipy.signal import find_peaks,peak_widths,peak_prominences,savgol_filter
from analysis import *
from tqdm import tqdm

folder_path = Path(r"C:\Users\mg942\Desktop\元澄\PIC9-FPN3_DOE1_MRM033_DC&RF_3dB\20260202")
db_path = Path(r"C:\Users\mg942\Desktop\元澄\Data") / "DataBase.db"
#%%
with DatabaseAPI(db_path) as db:
    db.import_session_folder(folder_path,schema_file="schema.sql")
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

# %%
import matplotlib.pyplot as plt
import time
from pathlib import Path
import pandas as pd
%matplotlib qt
with DatabaseAPI(db_path) as db:
    spcm_data = db.select_rawdata_by_session_id(measure_id = 5, data_type="SPCM")
    data_info = [(id,db.select_datainfo_dict_by_data_id(id)) for id in [d['data_id'] for d in spcm_data]]
    spcm = []
    for spcm_info in spcm_data:
        filename = Path(spcm_info['file_path']).name
        meta = db.parse_filename(filename)
        meta.pop('arguments')
        smu = meta.pop('SMU', [])
        meta |= dict(item for d in smu for item in d.items())
        for key, value in list(meta.items()):
            if all(x in key for x in ('ec','value')):
                meta[key.split()[0]+' unit'] = meta[key][1]
                meta[key] = meta[key][0]
        spcm += [meta]
    df = pd.DataFrame(spcm,index=[d['data_id'] for d in spcm_data])
    pn = [ec for ec in {col.split()[0] for col in df.columns if 'ec' in col}
          if any(df[f'{ec} type'] == 'pn')][0]
    heat = [ec for ec in {col.split()[0] for col in df.columns if 'ec' in col}
            if any(df[f'{ec} type'] == 'heat')][0]
    df_pn = df[df[f'{pn} type'] == 'pn']
    df_heat = df[df[f'{heat} type'] == 'heat']
    #pn modulation
    df_pn_grouped = df_pn.groupby([col for col in df_pn.columns if 'ec' not in col])
    # if len(df_pn_grouped) > 1:
    #     print("Warning: Multiple groups found in pn data. Please select the appropriate group.")
    #     for i, (name, _) in enumerate(df_pn_grouped):
    #         print(f"Group {i}: {name}")
    #     select_idx = input("Enter the group number to select: ")
    #     select_idx = [int(i) for i in select_idx.split(',')]
    # else:
    #     select_idx = [0]

    for i, (name, group) in enumerate(df_pn_grouped):
        group = group.sort_values(by=f'{pn} value')
        data_id = group.index.values
        if len(group) > 1:
            value = group[f'{pn} value'].values
            vh_id = int(data_id[np.argmax(np.abs(value))])
            v0_id = int(data_id[np.argmin(np.abs(value))])
            vh_path = db.select_rawdata_by_data_id(vh_id)['file_path']
            v0_path = db.select_rawdata_by_data_id(v0_id)['file_path']
            res = MRM_OMA_analysis(vh_path, v0_path, start=1305, end=1315)

    
    #df_heat_grouped = df_heat.groupby([col for col in df_heat.columns if 'ec' not in col])
    
#%%    
    cmd = '''
    SELECT md.data_id FROM RawDataFiles md
    JOIN MeasureSession ms ON md.session_id = ms.session_id
    JOIN DataInfo di ON md.data_id = di.data_id
    WHERE ms.measure_id = 1
    AND md.data_type = 'SPCM'
    AND (
    (di.Info_key = 'ec1 type'    AND di.Info_value = 'pn') OR
    (di.Info_key = 'ec1 channel' AND di.Info_value = '1')  OR
    (di.Info_key = 'ec1 value'   AND di.Info_value = '-900.0')
    )
    GROUP BY md.data_id
    HAVING COUNT(DISTINCT di.Info_key) = 3;'''
    a = db.query(cmd)
    
# %%
