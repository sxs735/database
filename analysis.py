import csv,re
import numpy as np
import inspect
from scipy.signal import find_peaks,peak_widths,peak_prominences,savgol_filter

def tofloat(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return np.nan

def mueller_to_loss(m11, m12, m13, m14):
    T_max = m11 + np.sqrt(m12**2 + m13**2 + m14**2)
    T_min = m11 - np.sqrt(m12**2 + m13**2 + m14**2)
    loss_max = -10 * np.log10(T_min)
    loss_min = -10 * np.log10(T_max)
    return loss_max, loss_min

def save_to_csv(path, rows, header=None):
    """Write tabular data to a CSV file using UTF-8 encoding."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if header is not None:
            writer.writerow(header)
        for row in rows:
            writer.writerow(row)

def save_spectrum_lite(setting,data, path):
    if setting['mode']  == 'min/max':
            header = ['wavelength(nm)']+ [f'DaqPort{port}_{suffix}' for port in setting['DaqPort'] for suffix in ['min', 'max']]
    else:
        header = ['wavelength(nm)']+ [f'DaqPort{port}' for port in setting['DaqPort']]
    save_to_csv(path, data, header=header)

def read_spectrum(path,start_idx=None,end_idx=None):
    pattern = re.compile(r'^DaqPort(\d+)$')
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        setting = {'DaqPort': []}
        data = []
        for i,row in enumerate(reader):
            if start_idx is None and '=== Min' in row:
                start_idx = i
                setting['mode'] = 'min/max'
            elif start_idx is None and '=== Average IL (TLS 0) ===' in row:
                start_idx = i
                setting['mode'] = 'average'
            elif end_idx is None and '=== Mueller Row 1 (TLS 0) ==='in row:
                break
            elif start_idx is not None:
                if i> start_idx:
                    data += [[tofloat(value) for value in row]]
            elif ('WavelengthStart' in row 
                  or 'WavelengthStop' in row 
                  or 'WavelengthStep' in row 
                  or 'SweepRate' in row):
                setting[row[0]] = f'{row[1]}({row[2]})'
            elif match := pattern.match(row[0]):
                setting['DaqPort'] += [match.group(1)]

        if start_idx is not None:
            data = np.array(data, dtype=float)
            data[:,0] *= 1E9
        else:
            print(path)
            raise ValueError("Don't support this file format")
    return setting, data

def read_spectrum_lite(path):
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        head = []
        data = []
        for i, row in enumerate(reader):
            if i==0:
                head = row
            else:
                data += [[tofloat(value) for value in row]]
    return head, np.array(data, dtype=float)

def read_spectrum_all(path):
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        mode = None
        header = []
        Min_Max = []
        Mueller = []
        Avg = []
        PDL = []
        TE_TM = []
        for i,row in enumerate(reader):
            #print(row)
            if '=== Min' in row and mode != 'min_max':
                mode = 'min_max'
            elif '=== Average IL (TLS 0) ===' in row and mode != 'average_il':
                mode = 'average_il'
            elif '=== Mueller Row 1 (TLS 0) ===' in row and mode != 'mueller':
                mode = 'mueller'
            elif '=== PDL (TLS 0) ===' in row and mode != 'pdl':
                mode = 'pdl'
            elif '=== TE' in row and mode != 'te_tm':
                mode = 'te_tm'
            elif mode == 'min_max':
                Min_Max += [[tofloat(value) for value in row]]
            elif mode == 'average_il':
                Avg += [[tofloat(value) for value in row]]
            elif mode == 'mueller':
                Mueller += [[tofloat(value) for value in row]]
            elif mode == 'pdl':
                PDL += [[tofloat(value) for value in row]]
            elif mode == 'te_tm':
                TE_TM += [[tofloat(value) for value in row]]
            else:
                header += [row]
        data = {'min_max': np.array(Min_Max),
                'average_il': np.array(Avg),
                'mueller': np.array(Mueller),
                'pdl': np.array(PDL),
                'te_tm': np.array(TE_TM),
                'header': header}
    return data

def exchange_2ports(file_path):
    old_csv = read_spectrum_all(file_path)
    for key in old_csv:
        if key == 'min_max':
            old_csv[key] = old_csv[key][:, [0, 3, 4, 1, 2]]
        elif key == 'average_il':
            old_csv[key] = old_csv[key][:, [0, 2, 1]]
        elif key == 'mueller':
            old_csv[key] = old_csv[key][:, [0, 5, 6, 7, 8, 1, 2, 3, 4]]
        elif key == 'pdl':
            old_csv[key] = old_csv[key][:, [0, 2, 1]]
        elif key == 'te_tm':
            old_csv[key] = old_csv[key][:, [0, 3, 4, 1, 2]]
        elif key == 'header':
            port = []
            for i, row in enumerate(old_csv[key]):
                match = re.search(r"DaqPort\d+", row[0])
                if match:
                    port += [i]
            pair = [(port[2*i],port[2*i+1]) for i in range(len(port)//2)]
            for i, j in pair:
                old_csv[key][i], old_csv[key][j] = old_csv[key][j], old_csv[key][i]

    new_csv = (old_csv['header'] + 
               [['=== Min','Max IL (TLS 0) ===']] +
               old_csv['min_max'].tolist() +
               [['=== Mueller Row 1 (TLS 0) ===']] +
               old_csv['mueller'].tolist() +
               [['=== Average IL (TLS 0) ===']] +
               old_csv['average_il'].tolist() +
               [['=== PDL (TLS 0) ===']] +
               old_csv['pdl'].tolist() +
               [['=== TE','TM (TLS 0) ===']] +
               old_csv['te_tm'].tolist())
    
    new_path = file_path.with_name(file_path.stem + "_new.csv")
    with open(new_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerows(new_csv)

def read_ssrf(path):
    """
    讀取 SSRF (S-parameter) 數據文件
    
    Parameters:
    -----------
    path : str or Path
        文件路徑
    
    Returns:
    --------
    head : dict
        標頭資訊字典
    data : ndarray
        數據陣列，包含 [freq(GHz), s11, s21, s12, s22]
    """
    
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        head = {}
        data = []
        in_header = True
        
        for i, row in enumerate(reader):
            if not row or not row[0]:  # 跳過空行
                continue
            # 檢查是否為標頭行
            if '#' in row[0] and in_header:
                in_header = False  # 標頭結束
                continue
            elif not in_header:
                row_data = row[0].split()
                data.append([tofloat(v) for v in row_data])
        
        data = np.array(data)
        
        # 確保數據形狀正確
        if data.shape[1] < 9:
            raise ValueError(f"數據列數不足，期望至少 9 列，實際 {data.shape[1]} 列")
        
        # 提取頻率和 S 參數
        freq = data[:, 0] / 1e9  # 轉換為 GHz
        s11 = data[:, 1] + 1j * data[:, 2]
        s21 = data[:, 3] + 1j * data[:, 4]
        s12 = data[:, 5] + 1j * data[:, 6]
        s22 = data[:, 7] + 1j * data[:, 8]
        
        # 組合成輸出格式（使用 complex128 類型）
        result = np.column_stack([freq, s11, s21, s12, s22])
    
    return result

def read_dcvi(path):
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if i==0:
                continue
            elif i>=1:
                data = row
            else:
                break
        data = {'channel':int(data[0]),
                'set mode':data[1],
                'set value': (tofloat(data[2]), 'V' if data[1]=="VOLT" else 'A'),
                'measured voltage': (tofloat(data[3]), 'V'),
                'measured current': (tofloat(data[4]), 'A')}
        return data

def MRM_SPCM_analysis(wavelength, loss, prominence=3, distance=5, baseline_order=3):
    """
    MRM SPCM 頻譜分析函數
    
    Parameters:
    -----------
    wavelength : array-like
        波長數據 (nm)
    loss : array-like
        損耗數據 (dB)
    prominence : float, optional
        峰值顯著性閾值 (default: 2)
    distance : int, optional
        峰值間最小距離 (default: 5)
    baseline_order : int, optional
        基線擬合的多項式階數 (default: 3)
    
    Returns:
    --------
    tuple : (result_dict, algorithm_name, version)
        result_dict  -> 包含 FSR、FWHM、Q factor 等分析參數的字典
        algorithm_name -> 字串，指出所使用的分析函數名稱
        version -> 字串，標記分析演算法版本
    """
    version = "1.0.0"
    # 檢查輸入數據
    if len(wavelength) != len(loss):
        raise ValueError("wavelength 和 loss 的長度必須相同")
    
    if len(wavelength) < 10:
        raise ValueError("數據點太少，無法進行分析")
    
    # 計算頻率 (THz)
    frequency = 299792.458 / wavelength
    
    # 基線校正
    baseline = np.polynomial.Polynomial.fit(wavelength, loss, baseline_order)
    loss_level = loss - baseline(wavelength)
    
    # 尋找谷值
    valley_idx, _ = find_peaks(-loss_level, prominence=prominence, distance=distance)

    wavelength_x = wavelength[valley_idx]
    frequency_x = frequency[valley_idx]
    valley_loss = loss_level[valley_idx]

    # 計算消光比 (Extinction Ratio)
    ER = peak_prominences(-loss_level, valley_idx)[0]
    
    # 計算 FWHM 和 Q factor
    Ty = 10 ** (loss_level / 10)
    wavelength_spacing = np.median(np.diff(wavelength))
    ips_to_wavelength = lambda x: wavelength_spacing*x + wavelength[0]
    widths, _, left_ips, right_ips = peak_widths(-Ty, valley_idx, rel_height=0.5)
    FWHMnm = widths * wavelength_spacing
    FWHMGHz = (1/ips_to_wavelength(left_ips)-1/ips_to_wavelength(right_ips))*299792458
    Q = wavelength_x / FWHMnm

    results = {'Extinction Ratio': (np.round(ER, 3).tolist(), 'dB'),
               'FWHM(nm)': (np.round(FWHMnm, 3).tolist(), 'nm'),
               'FWHM(GHz)': (np.round(FWHMGHz, 3).tolist(), 'GHz'),
               'Q factor': (np.round(Q, 0).tolist(), ''),
               'Valley Wavelength': (np.round(wavelength_x, 3).tolist(), 'nm'),
               'Valley Frequency': (np.round(frequency_x, 3).tolist(), 'THz')}

    # 檢查是否找到足夠的谷值
    if len(valley_idx) < 2 and len(valley_idx) > 0:
        return (results,
                inspect.currentframe().f_code.co_name, 
                version)
    
    # 計算 FSR (nm)
    FSRnm = wavelength_x[1:] - wavelength_x[:-1]
    FSRnm = np.vstack((np.r_[FSRnm, np.nan], np.r_[np.nan, FSRnm]))
    FSRidx = np.nanargmax(FSRnm, axis=0)
    FSRnm = np.nanmax(FSRnm, axis=0)
    
    # 計算 FSR (GHz)
    FSRGHz = frequency_x[:-1] - frequency_x[1:]
    FSRGHz = np.vstack((np.r_[FSRGHz, np.nan], np.r_[np.nan, FSRGHz]))
    FSRGHz = FSRGHz[FSRidx, range(len(FSRidx))]
    
    results.update({'FSR(nm)': (np.round(FSRnm, 3).tolist(), 'nm'),
                    'FSR(THz)': (np.round(FSRGHz, 3).tolist(), 'THz')})
    
    return (results,
            inspect.currentframe().f_code.co_name, 
            version)

def MRM_SSRF_analysis(frequency,
                      s21,
                      reference_frequency: float = 0.5,
                      drop_levels= 3,
                      smooth_window: int = 0,
                      polyorder: int = 2):
    """依據 S21 曲線估算小信號頻寬。

    Parameters
    ----------
    frequency : array-like
        頻率軸 (GHz)。演算法假設資料點已依頻率排序。
    s21 : array-like
        對應的 S21 幅度 (dB)。
    reference_frequency : float, optional
        作為 0 dB 參考點的頻率，預設 0.5 GHz。
    drop_levels : float, optional
        相對參考點欲偵測的衰減量，預設 3 dB。
    smooth_window : int, optional
        Savitzky-Golay 平滑視窗長度。小於 3 時跳過平滑。
    polyorder : int, optional
        Savitzky-Golay 多項式階數，需小於 smooth_window。

    Returns
    -------
    tuple
        (結果字典, 函式名稱, 版本)。
    """
    # s21 = 20*np.log10(np.abs(data[:,2]))
    version = "1.0.0"
    if smooth_window >= 3 and polyorder < smooth_window:
        smooth_s21 = savgol_filter(s21, smooth_window, polyorder)
    else:
        smooth_s21 = s21

    ref_idx = np.argmin(np.abs(frequency - reference_frequency))
    ref_loss = smooth_s21[ref_idx]

    between = ((smooth_s21[:-1]-(ref_loss-drop_levels))*(smooth_s21[1:]-(ref_loss-drop_levels))<=0) & (smooth_s21[:-1] != smooth_s21[1:])
    inter_point = frequency[:-1] + ((ref_loss-drop_levels) - smooth_s21[:-1]) * (frequency[1:] - frequency[:-1]) / (smooth_s21[1:] - smooth_s21[:-1])
    inter_x = inter_point[between]
    mask = inter_x > reference_frequency
    inter_x = inter_x[mask]
    if len(inter_x) > 0:
        frequency_x2 = np.min(inter_x[(inter_x - reference_frequency)>0])
        bandwidth = frequency_x2 - reference_frequency
    else:
        bandwidth = 0

    result = {'Bandwidth': (float(round(bandwidth, 3)), 'GHz')}

    return (result,
            inspect.currentframe().f_code.co_name,
            version)

def MRM_OMA_analysis(modulated_spcm, non_modulated_spcm, start=1305, end=1315):
    version = '1.0.0'

    start_idx = np.abs(non_modulated_spcm[:,0] - start).argmin()
    end_idx = np.abs(non_modulated_spcm[:,0] - end).argmin()
    modulated_spcm = modulated_spcm[start_idx:end_idx]
    non_modulated_spcm = non_modulated_spcm[start_idx:end_idx]

    ref_i = 3 if non_modulated_spcm.shape[1] == 5 else 2
    wavelength = non_modulated_spcm[:, 0]
    loss_vh =modulated_spcm[:, ref_i] - modulated_spcm[:, 1]
    loss_v0 =non_modulated_spcm[:, ref_i] - non_modulated_spcm[:, 1]
    T_vh = 10**(loss_vh/10)    
    T_v0 = 10**(loss_v0/10)

    diffT = T_vh - T_v0
    vaild_value = ~np.isnan(diffT)
    diffT = diffT[vaild_value]
    T_vh = T_vh[vaild_value]
    T_v0 = T_v0[vaild_value]
    loss_v0  = loss_v0[vaild_value]
    loss_vh  = loss_vh[vaild_value]
    wavelength = wavelength[vaild_value]
    #diffT = savgol_filter(diffT,17,3)
    oma_wl = wavelength[diffT.argmax()]
    valley_v0 = wavelength[loss_v0.argmin()]
    valley_vh = wavelength[loss_vh.argmin()]
    delta_wl = (valley_vh - valley_v0)*1000
    detuning =  oma_wl-valley_v0
    p0 = np.interp(oma_wl, wavelength, T_v0)
    p1 = np.interp(oma_wl, wavelength, T_vh)
    # import matplotlib.pyplot as plt
    # plt.plot(wavelength, T_vh, label='modulated')
    # plt.plot(wavelength, T_v0, label='non-modulated')
    # plt.plot([oma_wl,oma_wl],[p0,p1],'go')
    # plt.show()
    # print(p0,p1)
    roma = 10*np.log10(p1-p0)
    result = {'OMA Wavelength': (float(round(oma_wl,3)), 'nm'),
              'rOMA': (float(round(roma,3)), 'dB'),
              'Delta Wavelength': (float(round(delta_wl,3)), 'pm'),
              'Detuning': (float(round(detuning,3)), 'nm'),
              'Valley Wavelength 0': (float(round(valley_v0,3)), 'nm'),
              'Valley Wavelength 1': (float(round(valley_vh,3)), 'nm')}

    return (result,
            inspect.currentframe().f_code.co_name,
            version)

def MRM_tuning_analysis(modulated_spcm, non_modulated_spcm, start=1305, end=1315):
    version = '1.0.0'

    start_idx = np.abs(non_modulated_spcm[:,0] - start).argmin()
    end_idx = np.abs(non_modulated_spcm[:,0] - end).argmin()
    modulated_spcm = modulated_spcm[start_idx:end_idx]
    non_modulated_spcm = non_modulated_spcm[start_idx:end_idx]

    ref_i = 3 if non_modulated_spcm.shape[1] == 5 else 2
    wavelength = non_modulated_spcm[:, 0]
    loss_vh =modulated_spcm[:, ref_i] - modulated_spcm[:, 1]
    loss_v0 =non_modulated_spcm[:, ref_i] - non_modulated_spcm[:, 1]
    diffT = 10**(loss_vh/10)-10**(loss_v0/10)
    vaild_value = ~np.isnan(diffT)
    loss_v0  = loss_v0[vaild_value]
    loss_vh  = loss_vh[vaild_value]
    wavelength = wavelength[vaild_value]

    valley_v0 = wavelength[loss_v0.argmin()]
    valley_vh = wavelength[loss_vh.argmin()]
    delta_ghz = (299792.458/valley_v0 - 299792.458/valley_vh)*1000
    result = {'Delta Frequency': (float(round(delta_ghz,3)), 'GHz'),
              'Valley Wavelength 0': (float(round(valley_v0,3)), 'nm'),
              'Valley Wavelength 1': (float(round(valley_vh,3)), 'nm')}

    return (result,
            inspect.currentframe().f_code.co_name,
            version)

def Get_loss_at_wavelength(spcm, target_wavelength):
    version = "1.0.0"
    wavelength = spcm[:, 0]
    loss = spcm[:, 1]
    idx = np.argmin(np.abs(wavelength - target_wavelength))
    result = {'Wavelength': (float(round(wavelength[idx],3)), 'nm'),
              'Loss': (float(round(loss[idx],3)), 'dB')}
    return (result,
            inspect.currentframe().f_code.co_name, 
            version)