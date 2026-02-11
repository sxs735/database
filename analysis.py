import csv
import numpy as np
import inspect
from scipy.signal import find_peaks,peak_widths,peak_prominences,savgol_filter

def tofloat(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return np.nan

def read_spectrum(path,start_idx=None,end_idx=None):
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        head = {}
        data = []
        for i,row in enumerate(reader):
            if start_idx is None and ('=== Min' in row or '=== Average IL (TLS 0) ===' in row):
                start_idx = i
            elif end_idx is None and '=== Mueller Row 1 (TLS 0) ==='in row:
                break
            elif start_idx is not None:
                if i> start_idx:
                    data += [[tofloat(value) for value in row]]
            elif ('WavelengthStart' in row 
                  or 'WavelengthStop' in row 
                  or 'WavelengthStep' in row 
                  or 'SweepRate' in row):
                head[row[0]] = (row[1], row[2])

        if start_idx is not None:
            data = np.array(data, dtype=float)
            data[:,0] *= 1E9
        else:
            raise ValueError("Don't support this file format")
    return head, data

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

def MRM_SPCM_analysis(wavelength, loss, prominence=2, distance=5, baseline_order=3):
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
    
    # 檢查是否找到足夠的谷值
    if len(valley_idx) < 2:
        return {'Extinction Ratio': ([], 'dB'),
                'FSRnm': ([], 'nm'),
                'FSRGHz': ([], 'GHz'),
                'FWHMnm': ([], 'nm'),
                'FWHMGHz': ([], 'GHz'),
                'Q factor': ([], ''),
                'valley_wavelength': ([], 'nm'),
                'valley_frequency': ([], 'THz'),
                'valley_loss': ([], 'dB')}
    
    wavelength_x = wavelength[valley_idx]
    frequency_x = frequency[valley_idx]
    valley_loss = loss_level[valley_idx]
    
    # 計算 FSR (nm)
    FSRnm = wavelength_x[1:] - wavelength_x[:-1]
    FSRnm = np.vstack((np.r_[FSRnm, np.nan], np.r_[np.nan, FSRnm]))
    FSRidx = np.nanargmax(FSRnm, axis=0)
    FSRnm = np.nanmax(FSRnm, axis=0)
    
    # 計算 FSR (GHz)
    FSRGHz = frequency_x[:-1] - frequency_x[1:]
    FSRGHz = np.vstack((np.r_[FSRGHz, np.nan], np.r_[np.nan, FSRGHz]))
    FSRGHz = FSRGHz[FSRidx, range(len(FSRidx))]
    
    # 計算消光比 (Extinction Ratio)
    ER = -peak_prominences(-loss_level, valley_idx)[0]
    
    # 計算 FWHM 和 Q factor
    Ty = 10 ** (loss_level / 10)
    wavelength_spacing = np.median(np.diff(wavelength))
    frequency_spacing = -np.median(np.diff(frequency))
    
    FWHMnm = peak_widths(-Ty, valley_idx, rel_height=0.5)[0] * wavelength_spacing
    FWHMGHz = peak_widths(-Ty, valley_idx, rel_height=0.5)[0] * frequency_spacing
    Q = wavelength_x / FWHMnm
    
    return ({'Extinction Ratio': (ER, 'dB'),
            'FSRnm': (FSRnm.tolist(), 'nm'),
            'FSRGHz': (FSRGHz.tolist(), 'GHz'),
            'FWHMnm': (FWHMnm.tolist(), 'nm'),
            'FWHMGHz': (FWHMGHz.tolist(), 'GHz'),
            'Q factor': (Q.tolist(), ''),
            'valley_wavelength': (wavelength_x.tolist(), 'nm'),
            'valley_frequency': (frequency_x.tolist(), 'THz')},
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
    frequency_x2 = inter_point[between][0]
    bandwidth = frequency_x2 - reference_frequency

    result = {'bandwidth': (bandwidth, 'GHz')}

    return (result,
            inspect.currentframe().f_code.co_name,
            version)