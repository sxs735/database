from concurrent.futures import ThreadPoolExecutor
import re
import shutil
import sqlite3
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple, Union
from pathlib import Path

from analysis import *
from tqdm import tqdm

class DatabaseAPI:
    """基於 SQLite 的資料庫 API，用於管理光譜測量數據"""

    SUPPORTED_EXTENSIONS = {".csv", ".txt", ".s2p"}
    IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
    TABLE_DUT = "DUT"
    TABLE_MEASUREMENTS = "Measurement"
    TABLE_MEASURE_SESSIONS = "MeasureSession"
    TABLE_CONDITIONS = "Conditions"
    TABLE_DATA = "RawDataFiles"
    TABLE_OPTICAL_INFO = "OpticalInfo"
    TABLE_ELECTRIC_INFO = "ElectricInfo"
    TABLE_ANOTHER_INFO = "AnotherInfo"
    TABLE_ANALYSES = "Analyses"
    TABLE_ANALYSIS_SOURCES = "AnalysisSources"
    TABLE_FEATURES = "Features"
    TABLE_METRICS = "FeatureMetrics"
    RAW_DATA_FOLDER = "RawDataFiles"
    MAIN_PATTERN = re.compile(r"""
                              ^(?P<datatype>[^_]+)
                              _(?P<wafer>[^_]+)
                              _(?P<doe>[^_]+)
                              _(?P<cage>[^_]+)
                              _die(?P<die>\d+)
                              _(?P<temperature>-?\d+)C
                              _\#(?P<repeat>\d+)
                              _(?P<device>[^_]+)
                              _ch_(?P<ch_in>\d+)
                              _(?P<ch_out>\d+)
                              _(?P<power>-?\d+)dBm
                              (?P<rest>.*)
                              \.(?:csv|txt|s2p)$""",
                              re.VERBOSE)
    
    def __init__(self, db_path: str = "measurement_data.db"):
        """
        初始化資料庫連接
        
        Args:
            db_path: 資料庫文件路徑
        """
        self.db_path = db_path
        self.conn = None
        
    def connect(self):
        """建立資料庫連接"""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row  # 讓查詢結果可以像字典一樣訪問
        self.conn.execute("PRAGMA foreign_keys = ON")  # 啟用外鍵約束
        
    def close(self):
        """關閉資料庫連接"""
        if self.conn:
            self.conn.close()
            
    def __enter__(self):
        """Context manager 支援"""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager 支援"""
        self.close()
    
    @staticmethod
    def _validate_identifier(name: str, kind: str) -> None:
        """確保資料表或欄位名稱為合法的 SQL 標識符"""
        if not DatabaseAPI.IDENTIFIER_PATTERN.match(name):
            raise ValueError(f"Invalid {kind}: {name}. Use letters, numbers, and underscores only, starting with a letter or underscore.")

    @staticmethod
    def _normalize_timestamp(value: Optional[Union[datetime, int, float]]) -> datetime:
        """將輸入統一為秒級精度的 datetime 物件"""
        if value is None:
            return datetime.now().replace(microsecond=0)
        if isinstance(value, datetime):
            return value.replace(microsecond=0)
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value).replace(microsecond=0)
        raise TypeError(f"Unsupported timestamp type: {type(value)!r}")

    @staticmethod
    def _normalize_timestamp_for_query(value: Optional[Union[datetime, int, float, str]]) -> Optional[str]:
        """將輸入轉換為 ISO 格式字串供 SQL 查詢使用，僅在提供值時處理。"""
        if value is None:
            return None
        if isinstance(value, str):
            value = datetime.fromisoformat(value)
        normalized = DatabaseAPI._normalize_timestamp(value)
        return normalized.isoformat(sep=" ")

    @staticmethod
    def _coerce_db_value(raw: Any) -> Any:
        """嘗試將 TEXT 欄位轉換回數字，否則維持原值"""
        if isinstance(raw, str):
            stripped = raw.strip()
            if stripped == "":
                return ""
            try:
                numeric = float(stripped)
            except ValueError:
                return raw
            return int(numeric) if numeric.is_integer() else numeric
        return raw
   
    # =========================
    # 1. 創建資料庫
    # =========================
    
    def create_db(self, schema_file: str = "schema.sql"):
        """
        根據 schema.sql 創建資料庫結構
        
        Args:
            schema_file: schema 文件路徑
        """
        self.connect()
        
        # 讀取 schema 文件
        schema_path = Path(schema_file)
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema 文件不存在: {schema_file}")
            
        with open(schema_file, 'r', encoding='utf-8') as f:
            schema_sql = f.read()
        
        # 執行 schema
        self.conn.executescript(schema_sql)
        self.conn.commit()
        print(f"資料庫已創建: {self.db_path}")
        
    def reset_db(self, schema_file: str = "schema.sql"):
        """
        重置資料庫（刪除所有表並重新創建）
        
        Args:
            schema_file: schema 文件路徑
        """
        if self.conn:
            self.close()
            
        # 刪除現有資料庫文件
        db_file = Path(self.db_path)
        if db_file.exists():
            db_file.unlink()
            print(f"已刪除舊資料庫: {self.db_path}")
        
        # 重新創建
        self.create_db(schema_file)
    
    # =========================
    # 2. 寫入資料 (INSERT/UPDATE)
    # =========================
    
    # DUT 表
    def insert_dut(self,
                   wafer: str,
                   doe: str,
                   die: int,
                   cage: str,
                   device: str,
                   commit: bool = True) -> int:
        """
        插入 DUT 記錄
        
        Args:
            wafer: 晶圓編號
            doe: DOE 設計編號
            die: 晶粒編號
            cage: 籠編號
            device: 裝置編號
            
        Returns:
            DUT_id
        """
        cursor = self.conn.execute("""INSERT INTO DUT 
                   (wafer, DOE, die, cage, device)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT (wafer, DOE, die, cage, device) DO UPDATE SET
                   wafer = excluded.wafer   -- no-op update
                   RETURNING DUT_id""",
                   (wafer, doe, die, cage, device))
        DUT_id = cursor.fetchone()["DUT_id"]
        if commit:
            self.conn.commit()
        return DUT_id
  
    # Measurement 表
    def insert_measurement(self, 
                           dut_id: int,
                           session_name: Optional[str] = None,
                           operator: Optional[str] = None,
                           system: Optional[str] = None,
                           notes: Optional[str] = None,
                           measured_at: Optional[Union[datetime, int, float]] = None,
                           commit: bool = True) -> int:
        """
        插入測量會話記錄
        
        Args:
            dut_id: DUT ID
            session_name: 會話名稱
            measured_at: 測試時間（默認為當前時間）
            operator: 操作員
            system: 測試系統
            notes: 備註
            
        Returns:
            session_id
        """
        timestamp = self._normalize_timestamp(measured_at).isoformat(sep=" ")

        cursor = self.conn.execute(f"""INSERT INTO {self.TABLE_MEASUREMENTS} 
                                   (DUT_id, measure_name, measured_at, operator, system, notes)
                                   VALUES (?, ?, ?, ?, ?, ?)
                                   ON CONFLICT (DUT_id, measure_name) DO UPDATE SET
                                   measured_at = excluded.measured_at,
                                   operator = excluded.operator,
                                   system = excluded.system,
                                   notes = excluded.notes
                                   RETURNING measure_id""",
                                  (dut_id, session_name, timestamp, operator, system, notes))
        measure_id = cursor.fetchone()["measure_id"]
        if commit:
            self.conn.commit()
        return measure_id

    # MeasureSession 表
    def insert_session(self,
                       measure_id: int,
                       session_idx: int,
                       commit: bool = True) -> int:
        """確保 Measurement 下的 session 記錄存在並回傳 MeasureSession.session_id"""
        cursor = self.conn.execute(f"""INSERT INTO {self.TABLE_MEASURE_SESSIONS}
                                   (measure_id, session_idx)
                                   VALUES (?, ?)
                                   ON CONFLICT (measure_id, session_idx) DO UPDATE SET
                                   session_idx = excluded.session_idx
                                   RETURNING session_id""",
                                   (measure_id, session_idx))
        session_id = cursor.fetchone()["session_id"]
        if commit:
            self.conn.commit()
        return session_id
    
    # Conditions 表
    def insert_conditions(self, measure_id: int, conditions: Dict[str, Any], commit: bool = True):
        """
        批量插入實驗條件
        
        Args:
            measure_id: Measurement ID
            conditions: 條件字典 {'temperature': 25.0, 'voltage': 3.3, ...}
                       或包含單位的字典 {'temperature': (25.0, '°C'), 'voltage': (3.3, 'V'), ...}
        """
        for key, value_data in conditions.items():
            # 支援兩種格式：純數值 或 (數值, 單位) 元組
            if isinstance(value_data, (tuple, list)) and len(value_data) == 2:
                value, unit = value_data
            else:
                value, unit = value_data, None
            
            self.conn.execute(f"""INSERT INTO {self.TABLE_CONDITIONS} 
                              (measure_id, setting_parameters, setting_value, parameters_unit)
                              VALUES (?, ?, ?, ?)
                              ON CONFLICT (measure_id, setting_parameters, parameters_unit) DO UPDATE SET
                              setting_value = excluded.setting_value""",
                              (measure_id, key, value, unit))
        if commit:
            self.conn.commit()
    
    # RawDataFiles 表
    def insert_rawdata_file(self,
                            session_id: int,
                            data_type: str,
                            file_path: str,
                            file_name: Optional[str] = None,
                            recorded_at: Optional[Union[datetime, int, float]] = None,
                            commit: bool = True) -> int:
        """
        插入測量數據記錄
        
        Args:
            session_id: MeasureSession ID
            data_type: 數據類型（如 'spectrum'）
            file_path: 文件路徑
            file_name: 檔名（預設取自 file_path）
            recorded_at: 紀錄時間（默認為當前時間）
            
        Returns:
            data_id
        """
        recorded_at = self._normalize_timestamp(recorded_at).isoformat(sep=" ")
        resolved_name = file_name or Path(file_path).name

        #session_id = self.insert_session(measure_id, session_idx, commit=False)
        cursor = self.conn.execute(f"""INSERT INTO {self.TABLE_DATA} 
                                   (session_id, data_type, file_name, file_path, recorded_at)
                                   VALUES (?, ?, ?, ?, ?)
                                   ON CONFLICT (session_id, file_name) DO UPDATE SET
                                   data_type = excluded.data_type,
                                   file_path = excluded.file_path,
                                   recorded_at = excluded.recorded_at
                                   RETURNING data_id""",
                                   (session_id, data_type, resolved_name, file_path, recorded_at))
        data_id = cursor.fetchone()["data_id"]
        if commit:
            self.conn.commit()
        return data_id

    # OpticalInfo / ElectricInfo / AnotherInfo
    def insert_optical_info(self,
                            data_id: int,
                            input_channel: str,
                            output_channel: str,
                            input_power: str,
                            wavelength_start: str,
                            wavelength_stop: str,
                            sweep_rate: str,
                            commit: bool = True) -> None:
        """插入或更新 OpticalInfo（單筆資料對應一行）。"""
        cursor = self.conn.execute(f"""INSERT INTO {self.TABLE_OPTICAL_INFO}
                                   (data_id, input_channel, output_channel, input_power, wavelengthStart, wavelengthStop, sweepRate)
                                   VALUES (?, ?, ?, ?, ?, ?, ?)
                                   ON CONFLICT(data_id) DO UPDATE SET
                                   input_channel = excluded.input_channel,
                                   output_channel = excluded.output_channel,
                                   input_power = excluded.input_power,
                                   wavelengthStart = excluded.wavelengthStart,
                                   wavelengthStop = excluded.wavelengthStop,
                                   sweepRate = excluded.sweepRate
                                   RETURNING data_id""",
                                   (data_id, input_channel, output_channel, input_power, wavelength_start, wavelength_stop, sweep_rate))
        data_id = cursor.fetchone()["data_id"]
        if commit:
            self.conn.commit()
        return data_id

    def insert_electric_info(self,
                             data_id: int,
                             element: str,
                             channel: str,
                             set_mode: str,
                             set_value: str,
                             commit: bool = True) -> None:
        """插入或更新 ElectricInfo（單筆資料對應一行）。"""
        cursor = self.conn.execute(f"""INSERT INTO {self.TABLE_ELECTRIC_INFO}
                                   (data_id, element, channel, set_mode, set_value)
                                   VALUES (?, ?, ?, ?, ?)
                                   ON CONFLICT (data_id, channel) DO UPDATE SET
                                   element = excluded.element,
                                   set_mode = excluded.set_mode,
                                   set_value = excluded.set_value
                                   RETURNING data_id""",
                                   (data_id, element, channel, set_mode, set_value))
        data_id = cursor.fetchone()["data_id"]
        if commit:
            self.conn.commit()
        return data_id

    def insert_another_info(self, 
                            data_id: int, 
                            info_key: str, 
                            info_value: str, 
                            commit: bool = True) -> None:
        """插入其他自由格式資訊（key/value，可含單位）。"""

        cursor = self.conn.execute(f"""INSERT INTO {self.TABLE_ANOTHER_INFO}
                                   (data_id, info_key, info_value)
                                   VALUES (?, ?, ?)
                                   ON CONFLICT (data_id, info_key) DO UPDATE SET
                                   info_value = excluded.info_value
                                   RETURNING data_id""",
                                   (data_id, info_key, info_value))
        data_id = cursor.fetchone()["data_id"]
        if commit:
            self.conn.commit()
        return data_id
    
    # Analyses 表
    def insert_analysis(self,
                        session_id: int,
                        analysis_type: str,
                        instance_no: int,
                        algorithm: Optional[str] = None,
                        version: Optional[str] = None,
                        created_time: Optional[datetime] = None,
                        commit: bool = True) -> int:
        """
        插入分析執行記錄
        
        Args:
            session_id: MeasureSession ID
            analysis_type: 分析類型（如 'peak_detection'）
            instance_no: 分析實例編號
            algorithm: 具體演算法名稱
            version: 演算法版本
            created_time: 創建時間（默認為當前時間）
            
        Returns:
            analysis_id
        """
        if created_time is None:
            created_time = datetime.now().replace(microsecond=0).isoformat(sep=" ")
        if algorithm is None:
            algorithm = "unspecified"
        if version is None:
            version = "1.0.0"

        cursor = self.conn.execute(f"""INSERT INTO {self.TABLE_ANALYSES} 
                                   (session_id, analysis_type, instance_no, algorithm, version, created_time)
                                   VALUES (?, ?, ?, ?, ?, ?)
                                   ON CONFLICT (session_id, analysis_type, instance_no) DO UPDATE SET
                                   algorithm = excluded.algorithm,
                                   version = excluded.version,
                                   created_time = excluded.created_time
                                   RETURNING analysis_id""",
                                   (session_id, analysis_type, instance_no, algorithm, version, created_time))
        analysis_id = cursor.fetchone()["analysis_id"]
        if commit:
            self.conn.commit()
        return analysis_id

    # AnalysisSources 表
    def insert_sources(self, analysis_id: int, data_id: int, commit: bool = True) -> None:
        """建立分析與測量數據的對應"""
        self.conn.execute(f"""INSERT OR IGNORE INTO {self.TABLE_ANALYSIS_SOURCES}
                          (analysis_id, data_id)
                          VALUES (?, ?)""",
                          (analysis_id, data_id))
        if commit:
            self.conn.commit()
    
    # Features 表
    def insert_feature(self,analysis_id: int,feature_type: str,feature_idx: int, commit: bool = True) -> int:
        """
        插入分析特徵記錄
        
        Args:
            analysis_id: 分析 ID
            feature_type: 特徵類型（如 'peak', 'valley'）
            feature_idx: 特徵索引（對應 schema 中的 feature_idx）
            
        Returns:
            feature_id
        """
        cursor = self.conn.execute(f"""INSERT INTO {self.TABLE_FEATURES} 
                                   (analysis_id, feature_type, feature_idx)
                                   VALUES (?, ?, ?)
                                   ON CONFLICT (analysis_id, feature_type, feature_idx) DO UPDATE SET
                                   feature_idx = excluded.feature_idx
                                   RETURNING feature_id""",
                                   (analysis_id, feature_type, feature_idx))
        feature_id = cursor.fetchone()["feature_id"]
        if commit:
            self.conn.commit()
        return feature_id
    
    # FeatureMetrics 表
    def insert_metrics(self, feature_id: int, values: Dict[str, Any], commit: bool = True):
        """
        批量插入特徵值
        
        Args:
            feature_id: 特徵 ID
            values: 值字典 {'wavelength': 1550.0, 'intensity': 100.0, ...}
                   或包含單位的字典 {'wavelength': (1550.0, 'nm'), 'intensity': (100.0, 'dBm'), ...}
        """
        for key, value_data in values.items():
            # 支援兩種格式：純數值 或 (數值, 單位) 元組
            if isinstance(value_data, (tuple, list)) and len(value_data) == 2:
                value, unit = value_data
            else:
                value, unit = value_data, None
            
            self.conn.execute(f"""INSERT INTO {self.TABLE_METRICS} 
                              (feature_id, metric_key, metric_value, metric_unit)
                              VALUES (?, ?, ?, ?)
                              ON CONFLICT (feature_id, metric_key) DO UPDATE SET
                              metric_value = excluded.metric_value""",
                              (feature_id, key, value, unit))
        if commit:
            self.conn.commit()
    
    # =========================
    # 3. 查詢資料 (SELECT)
    # =========================
    
    def query(self, sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """
        執行查詢並返回結果
        
        Args:
            sql: SQL 查詢語句
            params: 查詢參數
            
        Returns:
            結果列表（每個元素為字典）
        """
        cursor = self.conn.execute(sql, params)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    
    # DUT 查詢 
    def select_duts(self,
                    dut_id: Optional[int] = None,
                    wafer: Optional[str] = None,
                    doe: Optional[str] = None,
                    die: Optional[int] = None,
                    cage: Optional[str] = None,
                    device: Optional[str] = None) -> List[Dict[str, Any]]:
        """依條件篩選 DUT，參數為 None 時表示不套用該條件。"""
        sql = f"SELECT * FROM {self.TABLE_DUT} WHERE 1=1"
        params: List[Any] = []

        if dut_id is not None:
            sql += " AND DUT_id = ?"
            params.append(dut_id)
        if wafer is not None:
            sql += " AND wafer = ?"
            params.append(wafer)
        if doe is not None:
            sql += " AND DOE = ?"
            params.append(doe)
        if die is not None:
            sql += " AND die = ?"
            params.append(die)
        if cage is not None:
            sql += " AND cage = ?"
            params.append(cage)
        if device is not None:
            sql += " AND device = ?"
            params.append(device)

        sql += " ORDER BY DUT_id"
        return self.query(sql, tuple(params))

    # Measurement 查詢
    def select_measurements(self,
                            dut_id: Optional[int] = None,
                            measure_name: Optional[str] = None,
                            measured_at_start: Optional[Union[datetime, int, float, str]] = None,
                            measured_at_end: Optional[Union[datetime, int, float, str]] = None) -> List[Dict[str, Any]]:
        """依 DUT、量測名稱與時間範圍篩選 Measurement。"""

        sql = f"SELECT * FROM {self.TABLE_MEASUREMENTS} WHERE 1=1"
        params: List[Any] = []

        if dut_id is not None:
            sql += " AND DUT_id = ?"
            params.append(dut_id)
        if measure_name is not None:
            sql += " AND measure_name = ?"
            params.append(measure_name)

        start_time = self._normalize_timestamp_for_query(measured_at_start)
        end_time = self._normalize_timestamp_for_query(measured_at_end)

        if start_time is not None:
            sql += " AND measured_at >= ?"
            params.append(start_time)
        if end_time is not None:
            sql += " AND measured_at <= ?"
            params.append(end_time)

        sql += " ORDER BY measured_at, measure_id"
        return self.query(sql, tuple(params))

    def select_session(self,
                       wafer: Optional[str] = None,
                       doe: Optional[str] = None,
                       die: Optional[int] = None,
                       cage: Optional[str] = None,
                       device: Optional[str] = None,
                       measure_name: Optional[str] = None,
                       measured_at_start: Optional[Union[datetime, int, float, str]] = None,
                       measured_at_stop: Optional[Union[datetime, int, float, str]] = None,
                       session_idx: Optional[int] = None) -> List[int]:
        """依 DUT、量測資訊與時間/序號條件取得 session_id 列表。"""

        sql = f"""SELECT ms.session_id
                  FROM {self.TABLE_MEASURE_SESSIONS} ms
                  JOIN {self.TABLE_MEASUREMENTS} m ON ms.measure_id = m.measure_id
                  JOIN {self.TABLE_DUT} d ON m.DUT_id = d.DUT_id
                  WHERE 1=1"""
        params: List[Any] = []

        if wafer is not None:
            sql += " AND d.wafer = ?"
            params.append(wafer)
        if doe is not None:
            sql += " AND d.DOE = ?"
            params.append(doe)
        if die is not None:
            sql += " AND d.die = ?"
            params.append(die)
        if cage is not None:
            sql += " AND d.cage = ?"
            params.append(cage)
        if device is not None:
            sql += " AND d.device = ?"
            params.append(device)
        if measure_name is not None:
            sql += " AND m.measure_name = ?"
            params.append(measure_name)

        start_time = self._normalize_timestamp_for_query(measured_at_start)
        stop_time = self._normalize_timestamp_for_query(measured_at_stop)

        if start_time is not None:
            sql += " AND m.measured_at >= ?"
            params.append(start_time)
        if stop_time is not None:
            sql += " AND m.measured_at <= ?"
            params.append(stop_time)
        if session_idx is not None:
            sql += " AND ms.session_idx = ?"
            params.append(session_idx)

        sql += " ORDER BY ms.session_id, ms.session_idx"
        return self.query(sql, tuple(params))

    # Conditions 查詢
    def select_conditions(self, measure_id: int) -> List[Dict[str, Any]]:
        """查詢特定會話的所有實驗條件"""
        return self.query(f"SELECT * FROM {self.TABLE_CONDITIONS} WHERE measure_id = ?",(measure_id,))
    
    # RawDataFiles 查詢
    def select_rawdata_files(self,
                             session_id: Optional[int] = None,
                             data_type: Optional[str] = None,
                             optical_input_channel: Optional[str] = None,
                             optical_output_channel: Optional[str] = None,
                             optical_input_power: Optional[str] = None,
                             electric_element: Optional[str] = None,
                             electric_channel: Optional[str] = None) -> List[Dict[str, Any]]:
        """依照資料、光學與電性條件篩選 RawDataFiles。"""

        sql = f"""SELECT rd.*,
                           oi.input_channel AS optical_input_channel,
                           oi.output_channel AS optical_output_channel,
                           oi.input_power AS optical_input_power,
                           GROUP_CONCAT(DISTINCT ei.element) AS electric_element,
                           GROUP_CONCAT(DISTINCT ei.channel) AS electric_channel
                    FROM {self.TABLE_DATA} rd
                    LEFT JOIN {self.TABLE_OPTICAL_INFO} oi ON rd.data_id = oi.data_id
                    LEFT JOIN {self.TABLE_ELECTRIC_INFO} ei ON rd.data_id = ei.data_id
                    WHERE 1=1"""
        params: List[Any] = []

        if session_id is not None:
            sql += " AND rd.session_id = ?"
            params.append(session_id)
        if data_type is not None:
            sql += " AND rd.data_type = ?"
            params.append(data_type)
        if optical_input_channel is not None:
            sql += " AND oi.input_channel = ?"
            params.append(optical_input_channel)
        if optical_output_channel is not None:
            sql += " AND oi.output_channel = ?"
            params.append(optical_output_channel)
        if optical_input_power is not None:
            sql += " AND oi.input_power = ?"
            params.append(optical_input_power)
        if electric_element is not None:
            sql += " AND ei.element = ?"
            params.append(electric_element)
        if electric_channel is not None:
            sql += " AND ei.channel = ?"
            params.append(electric_channel)

        #sql += " ORDER BY rd.data_id"
        sql += " GROUP BY rd.data_id ORDER BY rd.data_id"
        rows = self.query(sql, tuple(params))
        for r in rows:
            for f in ["electric_element", "electric_channel"]:
                if r[f]:
                    r[f] = r[f].split(",")
                else:
                    r[f] = []
        return rows

    # OpticalInfo / ElectricInfo 查詢
    def select_optical(self, data_id: int) -> Optional[Dict[str, Any]]:
        """取得單筆數據的光學設定。"""
        rows = self.query(f"SELECT * FROM {self.TABLE_OPTICAL_INFO} WHERE data_id = ?", (data_id,))
        return rows[0] if rows else None

    def select_electric(self, data_id: int) -> List[Dict[str, Any]]:
        """取得單筆數據的電性設定（每個 channel 一行）。"""
        return self.query(f"SELECT * FROM {self.TABLE_ELECTRIC_INFO} WHERE data_id = ? ORDER BY channel", (data_id,))
    
    def select_another(self, data_id: int) -> List[Dict[str, Any]]:
        """查詢自由格式的其它資訊。"""
        return self.query(f"SELECT * FROM {self.TABLE_ANOTHER_INFO} WHERE data_id = ?", (data_id,))

    # Analyses 查詢
    # AnalysisSources 查詢
    # Features 查詢
    # FeatureMetrics 查詢

    # =========================
    # 4. 刪除資料 (DELETE)
    # =========================
    
    def delete_record(self, table: str, record_id: int, commit: bool = True) -> int:
        TABLE_ID_MAP = {self.TABLE_DUT: "DUT_id",
                        self.TABLE_MEASUREMENTS: "measure_id",
                        self.TABLE_CONDITIONS: "condition_id",
                        self.TABLE_DATA: "data_id",
                        self.TABLE_OPTICAL_INFO: "data_id",
                        self.TABLE_ELECTRIC_INFO: "data_id",
                        self.TABLE_ANOTHER_INFO: "data_id",
                        self.TABLE_ANALYSES: "analysis_id",
                        self.TABLE_ANALYSIS_SOURCES: "analysis_id",
                        self.TABLE_FEATURES: "feature_id",
                        self.TABLE_METRICS: "metric_id"}
        
        id_column = TABLE_ID_MAP[table]
        query = f"DELETE FROM {table} WHERE {id_column} = ?"
        cursor = self.conn.execute(query, (record_id,))
        if commit:
            self.conn.commit()
        return cursor.rowcount
    
    def vacuum(self,
               into_path: Optional[Union[str, Path]] = None,
               checkpoint: Optional[str] = "TRUNCATE",
               optimize: bool = True,
               incremental_pages: Optional[int] = None) -> Optional[Path]:
        """壓縮資料庫檔案並清理 WAL，支援 VACUUM INTO 與 incremental vacuum。

        Args:
            into_path: 若提供則使用 `VACUUM INTO` 產生新的壓縮檔案。
                       目錄會自動建立，並覆寫既有檔案。
            checkpoint: 在 VACUUM 前執行的 WAL checkpoint 模式，可為
                        PASSIVE/FULL/RESTART/TRUNCATE。設定為 None 可跳過。
            optimize:  若為 True，於完成後執行 `PRAGMA optimize`。
            incremental_pages: 指定 `PRAGMA incremental_vacuum(n)` 的頁數，
                               僅在 auto_vacuum = incremental 時有效。

        Returns:
            into_path 轉換為 Path（使用 VACUUM INTO 時）或 None。
        """
        if self.conn is None:
            self.connect()
            close_after = True
        else:
            close_after = False

        try:
            self.conn.commit()  # sqlite 要求 VACUUM 在 auto-commit 模式下執行

            if checkpoint:
                mode = checkpoint.upper()
                valid_modes = {"PASSIVE", "FULL", "RESTART", "TRUNCATE"}
                if mode not in valid_modes:
                    raise ValueError(f"Unsupported checkpoint mode: {checkpoint}")
                self.conn.execute(f"PRAGMA wal_checkpoint({mode})")

            if incremental_pages is not None:
                if incremental_pages < 0:
                    raise ValueError("incremental_pages must be non-negative")
                self.conn.execute(f"PRAGMA incremental_vacuum({int(incremental_pages)})")

            target_path: Optional[Path] = None
            if into_path:
                target_path = Path(into_path)
                target_path.parent.mkdir(parents=True, exist_ok=True)
                self.conn.execute("VACUUM INTO ?", (str(target_path),))
            else:
                self.conn.execute("VACUUM")

            if optimize:
                self.conn.execute("PRAGMA optimize")

            self.conn.commit()
            return target_path
        finally:
            if close_after:
                self.close()
    
    # =========================
    # 輔助方法
    # =========================
    def add_column(self,
                   table_name: str,
                   column_name: str,
                   column_definition: str,
                   if_not_exists: bool = True,
                   commit: bool = True) -> bool:
        """在既有資料表中新增欄位。"""
        if not self.conn:
            raise RuntimeError("Database connection is not established. Use the context manager or call connect().")

        self._validate_identifier(table_name, "table name")
        self._validate_identifier(column_name, "column name")

        definition = column_definition.strip()
        if not definition:
            raise ValueError("column_definition must be a non-empty string, e.g. 'TEXT NOT NULL DEFAULT \"1\"'.")

        if if_not_exists:
            existing_columns = {row["name"] for row in self.conn.execute(f'PRAGMA table_info("{table_name}")')}
            if column_name in existing_columns:
                return False

        self.conn.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{column_name}" {definition}')
        
        if commit:
            self.conn.commit()
        return True

    def export_all_tables_to_xlsx(self, output_path: str = "database_export.xlsx") -> str:
        """
        將整個資料庫所有表輸出成 xlsx 檔案

        Args:
            output_path: 輸出檔案路徑

        Returns:
            輸出檔案路徑
        """
        try:
            import pandas as pd
        except ImportError as exc:
            raise ImportError("需要安裝 pandas 與 openpyxl：pip install pandas openpyxl") from exc

        tables = self.query("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")

        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            for row in tables:
                table_name = row['name']
                df = pd.read_sql_query(f"SELECT * FROM {table_name}", self.conn)
                sheet_name = table_name[:31]
                df.to_excel(writer, sheet_name=sheet_name, index=False)

        return output_path

    def backup_database(self,
                        backup_path: Optional[Union[str, Path]] = None) -> Path:
        """備份目前的 SQLite 檔案並回傳備份路徑。"""
        created_connection = False
        if self.conn is None:
            self.connect()
            created_connection = True

        try:
            source_path = Path(self.db_path)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            suffix = source_path.suffix or ".db"
            default_name = f"{source_path.stem}_backup_{timestamp}{suffix}"

            if backup_path is None:
                destination = source_path.parent / 'backup' / default_name
            else:
                candidate = Path(backup_path)
                if candidate.exists() and candidate.is_dir():
                    destination = candidate / default_name
                else:
                    destination = candidate

            destination.parent.mkdir(parents=True, exist_ok=True)

            with sqlite3.connect(destination) as backup_conn:
                self.conn.backup(backup_conn)

            return destination
        finally:
            if created_connection:
                self.close()

    def restore_database(self,
                         backup_path: Optional[Union[str, Path]] = None,
                         create_backup: bool = True) -> Optional[Path]:
        """以備份檔覆蓋目前資料庫；未提供路徑時自動使用最新備份。"""
        if backup_path is None:
            backup_dir = Path(self.db_path).parent / 'backup'
            if not backup_dir.exists() or not backup_dir.is_dir():
                raise FileNotFoundError("找不到備份資料夾，也無法推斷備份檔案。")
            try:
                backup_path = max(backup_dir.iterdir(), key=lambda p: p.stat().st_mtime)
            except ValueError as exc:
                raise FileNotFoundError("備份資料夾為空，無可用備份檔案。") from exc

        backup_path = Path(backup_path)
        if not backup_path.exists() or not backup_path.is_file():
            raise FileNotFoundError(f"找不到備份檔案: {backup_path}")

        current_db = Path(self.db_path)
        was_connected = self.conn is not None

        if was_connected:
            self.close()

        safety_backup: Optional[Path] = None
        if create_backup and current_db.exists():
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            suffix = current_db.suffix or ".db"
            safety_backup = current_db.with_name(f"{current_db.stem}_pre_restore_{timestamp}{suffix}")
            safety_backup.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(current_db, safety_backup)

        current_db.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(backup_path, current_db)

        if was_connected:
            self.connect()

        return safety_backup

    @classmethod
    def parse_filename(cls, filename: str) -> Dict[str, Any]:
        name = Path(filename).name
        match = cls.MAIN_PATTERN.match(name)

        if not match:
            raise ValueError(f"檔名格式不符: {filename}")

        result = match.groupdict()
        rest = result.pop("rest")

        result["SMU"] = []
        result["arguments"] = []

        if not rest:
            return result

        tokens = rest.strip("_").split("_")
        i = 0
        ec_i = 1
        arg_i = 1
        pass_SMU = False
        while i < len(tokens):
            token = tokens[i]
            if token == "SMU":
                match = re.match(r"([-+]?\d*\.?\d+)([a-zA-Z%]*)", tokens[i + 3])
                result["SMU"].append({"element": tokens[i + 1],
                                      "channel": tokens[i + 2],
                                      "set_mode": 'VOLT' if match[2] in ['V', 'mV'] else 'CURR',
                                      "set_value": tokens[i + 3]})
                i += 4
                ec_i += 1
                continue
            if i + 2 < len(tokens) and token != "arg" and not pass_SMU:
                match = re.match(r"([-+]?\d*\.?\d+)([a-zA-Z%]*)", tokens[i + 2])
                result["SMU"].append({"element": tokens[i],
                                      "channel": tokens[i + 1],
                                      "set_mode": 'VOLT' if match[2] in ['V', 'mV'] else 'CURR',
                                      "set_value": tokens[i + 2]})
                i += 3
                ec_i += 1
                continue
            if token == "arg":
                result["arguments"].append({f"arg": tokens[i + 1]})
                arg_i += 1
                i += 2
                pass_SMU = True
                continue
            if token:
                result["arguments"].append({"arg": token})
                arg_i += 1
            i += 1

        return result

    @classmethod
    def parse_folder(cls, folder_path: str) -> Tuple[Dict[Path, Dict[str, Any]], List[str]]:
        """
        檢測資料夾內所有檔案名稱是否符合指定格式

        Returns:
        - (valid_files, invalid_files) 元組
          - valid_files: {Path物件: 解析後的元數據字典, ...}
          - invalid_files: 不符合格式的檔案名稱列表
        """
        folder = Path(folder_path)

        if not folder.exists():
            raise FileNotFoundError(f"資料夾不存在: {folder_path}")

        valid_files: Dict[Path, Dict[str, Any]] = {}
        invalid_files: List[str] = []

        for ext in cls.SUPPORTED_EXTENSIONS:
            for file_path in folder.glob(f"*{ext}"):
                try:
                    meta = cls.parse_filename(file_path.name)
                    valid_files[file_path] = meta
                except ValueError:
                    invalid_files.append(file_path.name)
        if len(valid_files) == 0:
            print("資料夾內沒有符合格式的檔案。")
            print('testing filename parsing')
            try:
                file_path = list(folder.glob("*.csv"))[0]
            except IndexError:
                file_path = list(folder.glob("*.s2p"))[0]
            cls.test_filename_parsing(file_path.name)
            raise ValueError("資料夾內沒有符合格式的檔案。")
        return valid_files, invalid_files

    @classmethod
    def test_filename_parsing(cls,filename):
        patterns = [("datatype", r"^[^_]+"),
                    ("wafer", r"_[^_]+"),
                    ("doe", r"_[^_]+"),
                    ("cage", r"_[^_]+"),
                    ("die", r"_die\d+"),
                    ("temperature", r"_-?\d+C"),
                    ("repeat", r"_\#\d+"),
                    ("device", r"_[^_]+"),
                    ("channel", r"_ch_\d+_\d+"),
                    ("power", r"_-?\d+dBm"),
                    ("rest", r".*\.(?:csv|txt|s2p)$")]

        pos = 0
        for name, pattern in patterns:
            m = re.match(pattern, filename[pos:])
            if not m:
                print(f"Mismatch at {name}")
                break
            else:
                print(f"{name}: pass")
            pos += m.end()

    @classmethod
    def move_file(cls, src_dst):
        """Move a file unless the destination already exists."""
        src, dst = src_dst
        if dst.exists():
            return False
        try:
            src.rename(dst)  # 同磁碟機非常快
        except OSError:
            shutil.move(str(src), str(dst))  # 跨磁碟機降級
        return True

    @classmethod
    def copy_file(cls, src_dst):
        """Copy a file to destination; uses copy2 to preserve metadata."""
        src, dst = src_dst
        if dst.exists():
            return False
        shutil.copy2(src, dst)
        return True

    def import_from_measurement_folder(self, folder_path, schema_file="schema.sql"):
        """批次匯入資料夾中的測量檔案並寫入資料庫與 RawDataFiles 目錄。

        Args:
            folder_path: 含有測量檔案的資料夾路徑。
            schema_file: 在需要時用來初始化資料庫的 schema 檔案。

        Returns:
            None
        """

        folder = Path(folder_path)
        # 解析輸入資料夾，將符合命名規範的檔案與不合規檔案分開
        valid_files, invalid_files = self.parse_folder(folder)
        target_root_path = Path(self.db_path).parent / self.RAW_DATA_FOLDER

        try:
            # 若資料庫不存在則依 schema 初始化（已存在會拋例外並忽略）
            self.create_db(schema_file)
        except Exception:
            pass

        tested_timestamp = folder.stat().st_birthtime
        move_path = []
        try:
            # 使用交易確保整批匯入的原子性
            self.conn.execute("BEGIN")
            for filepath, file_info_raw in tqdm(valid_files.items(), desc="Importing", unit="file"):
                file_info = dict(file_info_raw)
                session_name = folder.name
                repeat_folder = r"#" + file_info["repeat"]
                session_idx = int(file_info["repeat"])
                # 依檔名資訊建立資料儲存層級，確保相同結構下存放原始檔
                target_dir = (target_root_path/ 
                              file_info["wafer"]/ 
                              file_info["doe"]/ 
                              file_info["cage"]/
                              file_info["device"]/
                              f"die{file_info['die']}"/ session_name/ repeat_folder)
                target_dir.mkdir(parents=True, exist_ok=True)
                dst = target_dir / filepath.name
                relative_dst = dst.relative_to(target_root_path.parent)

                # 寫入 DUT 與量測會話基礎資料
                dut_id = self.insert_dut(wafer=file_info["wafer"],
                                         doe=file_info["doe"],
                                         die=file_info["die"],
                                         cage=file_info["cage"],
                                         device=file_info["device"],
                                         commit=False)

                measure_id = self.insert_measurement(dut_id=dut_id,
                                                     session_name=session_name,
                                                     operator="T&P",
                                                     system="CM300v1.0",
                                                     measured_at=tested_timestamp,
                                                     notes="",
                                                     commit=False)
                
                self.insert_conditions(measure_id, {"temperature": (file_info["temperature"], "°C")}, commit=False)

                session_id = self.insert_session(measure_id, session_idx, commit=False)

                smu_entries = file_info.pop("SMU")
                arguments = file_info.pop("arguments")

                wavelength_start=None
                wavelength_stop=None
                sweep_rate=None

                if file_info["datatype"] in ["SPCM"]:
                    # SPCM 需要先解析內容以取得光學設定，同時另存壓縮版
                    setting, data = read_spectrum(filepath)
                    wavelength_start=setting["WavelengthStart"]
                    wavelength_stop=setting["WavelengthStop"]
                    sweep_rate=setting["SweepRate"]

                    move_path.append((filepath, dst))
                    filepath = filepath.parent / filepath.name.replace("SPCM", "SPCMs")
                    save_spectrum_lite(setting, data, filepath)
                    dst = target_dir / filepath.name
                    relative_dst = dst.relative_to(target_root_path.parent)


                data_id = self.insert_rawdata_file(session_id=session_id,
                                                   data_type=file_info["datatype"],
                                                   recorded_at=filepath.stat().st_mtime,
                                                   file_path=str(relative_dst),
                                                   file_name=filepath.name,
                                                   commit=False)

                # 儲存光學/電性設定與其它參數
                self.insert_optical_info(data_id=data_id,
                                         input_channel=file_info["ch_in"],
                                         output_channel=file_info["ch_out"],
                                         input_power=f"{file_info['power']} dBm",
                                         wavelength_start=wavelength_start,
                                         wavelength_stop=wavelength_stop,
                                         sweep_rate=sweep_rate,
                                         commit=False)
                for row in smu_entries:
                    self.insert_electric_info(data_id=data_id,
                                              element=row["element"],
                                              channel=row["channel"],
                                              set_mode=row["set_mode"],
                                              set_value=row["set_value"],
                                              commit=False)
                    
                for idx, row in enumerate(arguments):
                    self.insert_another_info(data_id=data_id,
                                             info_key=f'arg_{idx}',
                                             info_value=row["arg"],
                                             commit=False)

                move_path.append((filepath, dst))
            self.conn.commit()
            # 交易完成後移動或複製檔案，避免 I/O 影響 DB 寫入
            with ThreadPoolExecutor() as executor:
                # list(tqdm(executor.map(self.move_file, move_path), 
                #         total=len(move_path), 
                #         desc="Moving", 
                #         unit="file"))
                list(tqdm(executor.map(self.copy_file, move_path), 
                        total=len(move_path), 
                        desc="copying", 
                        unit="file"))
        except Exception:
            if self.conn:
                self.conn.rollback()
            raise
        print(f"匯入資料庫完成")
 
    def MRM_SPCM_analysis_by_session(self,session_id,input_channel=None,output_channel=None,commit=True):
        spcm_info = self.select_rawdata_files(session_id,data_type='SPCM',
                                              optical_input_channel= input_channel,
                                              optical_output_channel= output_channel)
        for instance_no, info in enumerate(spcm_info):
            filepath = Path(self.db_path).parent / info['file_path']
            head, data = read_spectrum_lite(filepath)
            x = data[:, 0]
            col = 3 if data.shape[1] == 5 else 2
            y = data[:, col] - data[:, 1]
            result, algorithm_name, version = MRM_SPCM_analysis(x, y,prominence=2.5)
            analysis_id = self.insert_analysis(session_id = session_id,
                                               analysis_type = 'MRM_SPCM_analysis',
                                               instance_no = instance_no,
                                               algorithm = algorithm_name,
                                               version = version,
                                               commit=commit)
            self.insert_sources(analysis_id, info["data_id"], commit=commit)
            for i in range(len(result['Valley Wavelength'][0])):
                feature_id = self.insert_feature(analysis_id=analysis_id, feature_type='Basic parameters', feature_idx=i, commit=commit)
                result_idx = {key:(result[key][0][i],result[key][1]) for key in result}
                self.insert_metrics(feature_id, result_idx, commit=commit)

    def MRM_OMA_analysis_by_session(self,session_id,start=1305, end=1315,commit=True):
        spcm_info = self.select_rawdata_files(session_id,data_type='SPCM')
        modulated_spcm = {}
        for data in spcm_info:
            electric_info = self.select_electric(data['data_id'])[0]
            if electric_info['element'] == 'pn':
                voltage = float(re.search(r'-?\d+\.?\d*', electric_info['set_value']).group())/1000
                modulated_spcm[voltage] = data
        if len(modulated_spcm) >= 2:
            modulated_voltage = np.array(list(modulated_spcm.keys()))
            non_modulated_idx = np.argmin(np.square(modulated_voltage))
            max_modulated_idx = np.argmax(np.square(modulated_voltage))
            non_modulated_info = modulated_spcm[modulated_voltage[non_modulated_idx]]
            max_modulated_info = modulated_spcm[modulated_voltage[max_modulated_idx]]

            non_modulated_path = non_modulated_info['file_path']
            max_modulated_path = max_modulated_info['file_path']
            
            _,modulated_spcm = read_spectrum_lite(Path(self.db_path).parent / max_modulated_path)
            _,non_modulated_spcm = read_spectrum_lite(Path(self.db_path).parent / non_modulated_path)
            result, algorithm_name, version = MRM_OMA_analysis(modulated_spcm, 
                                                                non_modulated_spcm, start=start, end=end)
            
            delta_modulated_voltage = abs(modulated_voltage[max_modulated_idx] - modulated_voltage[non_modulated_idx])
            modulated_efficiency = (result['Delta Wavelength'][0]/delta_modulated_voltage)
            result['Modulated Voltage'] = (float(round(delta_modulated_voltage,3)), 'V')
            result['Modulated Efficiency'] = (float(round(modulated_efficiency,3)), 'pm/V')
            analysis_id = self.insert_analysis(session_id = session_id,
                                                analysis_type = 'MRM_OMA_analysis',
                                                instance_no = 0,
                                                algorithm = algorithm_name,
                                                version = version,
                                                commit=commit)
            self.insert_sources(analysis_id, non_modulated_info["data_id"], commit=commit)
            self.insert_sources(analysis_id, max_modulated_info["data_id"], commit=commit)
            feature_id = self.insert_feature(analysis_id=analysis_id, feature_type='OMA parameters', feature_idx=0, commit=commit)
            result = {key:(result[key][0],result[key][1]) for key in result}
            self.insert_metrics(feature_id, result, commit=commit)

    def MRM_tuning_analysis_by_session(self,session_id,start=1305, end=1315,commit=True):
        spcm_info = self.select_rawdata_files(session_id,data_type='SPCM')
        dciv_info = self.select_rawdata_files(session_id, data_type='DCIV')
        
        spcm = {}
        for data in spcm_info:
            electric_info = self.select_electric(data['data_id'])[0]
            if electric_info['element'] =='heat':
                voltage = float(re.search(r'-?\d+\.?\d*', electric_info['set_value']).group())/1000
                spcm[voltage] = data
        dciv = {}
        for data in dciv_info:
            electric_info = self.select_electric(data['data_id'])[0]
            if electric_info['element'] =='heat':
                voltage = float(re.search(r'-?\d+\.?\d*', electric_info['set_value']).group())/1000
                electric_data = read_dcvi(Path(self.db_path).parent / data['file_path'])
                resistance = electric_data['measured voltage'][0]/electric_data['measured current'][0] if electric_data['measured current'][0] != 0 else float('inf')
                power = electric_data['measured voltage'][0]*electric_data['measured current'][0]*1000
                data['resistance'] = resistance
                data['power'] = power
                dciv[voltage] = data

        modulated_voltage = np.array(list(spcm.keys()))
        sorted_index = np.argsort(modulated_voltage)
        non_modulated_info = spcm[modulated_voltage[sorted_index[0]]]
        non_modulated_path = non_modulated_info['file_path']
        _,non_modulated_spcm = read_spectrum_lite(Path(self.db_path).parent / non_modulated_path)
        for no, idx in enumerate(sorted_index[1:]):
            voltage = modulated_voltage[idx]
            modulated_info = spcm[float(voltage)]
            modulated_path = modulated_info['file_path']
            dciv_info = dciv[voltage]
            _,modulated_spcm = read_spectrum_lite(Path(self.db_path).parent / modulated_path)
            result, algorithm_name, version = MRM_tuning_analysis(modulated_spcm, 
                                                                non_modulated_spcm, start=start, end=end)
            result['Tuning Efficiency'] = (round(result['Delta Frequency'][0]/dciv_info['power'],3), 'GHz/mW')
            result['Heater resistance'] = (round(dciv_info['resistance'], 3), 'Ohm')
        
            analysis_id = self.insert_analysis(session_id = session_id,
                                            analysis_type = 'MRM_tuning_analysis',
                                            instance_no = no,
                                            algorithm = algorithm_name,
                                            version = version,
                                            commit=commit)
            self.insert_sources(analysis_id, non_modulated_info["data_id"], commit=commit)
            self.insert_sources(analysis_id, modulated_info["data_id"], commit=commit)
            self.insert_sources(analysis_id, dciv_info["data_id"], commit=commit)
            feature_id = self.insert_feature(analysis_id=analysis_id, feature_type='Tuning parameters', feature_idx=0, commit=commit)
            result = {key:(result[key][0],result[key][1]) for key in result}
            self.insert_metrics(feature_id, result, commit=commit)

    def MRM_SSRF_analysis_by_session(self,session_id,commit=True):
        ssrf_info = self.select_rawdata_files(session_id, data_type='SSRF')
        input_powers = []
        for data in ssrf_info:
            input_powers += [float(self.select_optical(data['data_id'])['input_power']
                                   .replace(' dBm', ''))]
        sorted_index = np.argsort(input_powers)
        for no,idx in enumerate(sorted_index):
            ssrf_data = read_ssrf(Path(self.db_path).parent / ssrf_info[idx]['file_path'])
            frequency = np.real(ssrf_data[:,0])
            s21 = 20*np.log10(np.abs(ssrf_data[:,2]))
            result, algorithm_name, version = MRM_SSRF_analysis(frequency,s21, smooth_window=7,polyorder=2)
            
            analysis_id = self.insert_analysis(session_id = session_id,
                                               analysis_type = 'MRM_SSRF_analysis',
                                               instance_no = no,
                                               algorithm = algorithm_name,
                                               version = version,
                                               commit=commit)
            self.insert_sources(analysis_id, ssrf_info[idx]["data_id"], commit=commit)
            feature_id = self.insert_feature(analysis_id=analysis_id, feature_type='SSRF parameters', feature_idx=0, commit=commit)
            self.insert_metrics(feature_id, result, commit=commit)