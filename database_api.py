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
    MAIN_PATTERN = re.compile(r"""
                              ^(?P<datatype>[^_]+)
                              _(?P<wafer>[^_]+)
                              _(?P<doe>[^_]+)
                              _(?P<cage>[^_]+)
                              _die(?P<die>\d+)
                              _(?P<subdie>\d+)
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
    def select_dut_by_id(self, dut_id: int) -> Optional[Dict[str, Any]]:
        """根據 ID 查詢 DUT"""
        results = self.query("SELECT * FROM DUT WHERE DUT_id = ?", (dut_id,))
        return results[0] if results else None
    
    def select_list_duts(self, wafer: Optional[str] = None, die: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        查詢 DUT
        
        Args:
            wafer: 晶圓編號（可選）
            die: 晶粒編號（可選）
        """
        sql = "SELECT * FROM DUT WHERE 1=1"
        params = []
        
        if wafer:
            sql += " AND wafer = ?"
            params.append(wafer)
        if die is not None:
            sql += " AND die = ?"
            params.append(die)
            
        return self.query(sql, tuple(params))
    
    # Measurement 查詢
    def select_session_by_id(self, measure_id: int) -> Optional[Dict[str, Any]]:
        """根據 Measurement ID 查詢測量會話"""
        results = self.query(f"SELECT * FROM {self.TABLE_MEASUREMENTS} WHERE measure_id = ?", (measure_id,))
        return results[0] if results else None
    
    def select_session_ids_by_measure_name(self, measure_name: str) -> List[int]:
        """根據 measure_name 取得所有 session_id（依 session_idx 排序）。"""
        sql = f"""SELECT ms.session_id
                  FROM {self.TABLE_MEASURE_SESSIONS} ms
                  JOIN {self.TABLE_MEASUREMENTS} m ON ms.measure_id = m.measure_id
                  WHERE m.measure_name = ?
                  ORDER BY ms.session_idx"""
        rows = self.query(sql, (measure_name,))
        return [row["session_id"] for row in rows]
    
    def select_session_ids_by_measure_name_and_cage(self, measure_name: str, cage: str) -> List[int]:
        sql = f"""SELECT ms.session_id
                  FROM {self.TABLE_MEASURE_SESSIONS} ms
                  JOIN {self.TABLE_MEASUREMENTS} m ON ms.measure_id = m.measure_id
                  JOIN {self.TABLE_DUT} d ON m.DUT_id = d.DUT_id
                  WHERE m.measure_name = ?
                  AND d.cage = ?
                  ORDER BY ms.session_idx"""
        
        rows = self.query(sql, (measure_name, cage))
        return [row["session_id"] for row in rows]

    def select_sessions_by_dut_id(self, dut_id: int) -> List[Dict[str, Any]]:
        """查詢特定 DUT 的所有測量會話"""
        return self.query(f"SELECT * FROM {self.TABLE_MEASUREMENTS} WHERE DUT_id = ? ORDER BY measured_at DESC",(dut_id,))
    
    def select_sessions_by_date_range(self,start_date: datetime,end_date: datetime) -> List[Dict[str, Any]]:
        """查詢日期範圍內的測量會話"""
        return self.query(f"""SELECT * FROM {self.TABLE_MEASUREMENTS} 
                          WHERE measured_at BETWEEN ? AND ? ORDER BY measured_at DESC""",
                          (start_date, end_date))
    
    # Conditions 查詢
    def select_conditions_by_session_id(self, measure_id: int) -> List[Dict[str, Any]]:
        """查詢特定會話的所有實驗條件"""
        return self.query(f"SELECT * FROM {self.TABLE_CONDITIONS} WHERE measure_id = ?",(measure_id,))
    
    def select_conditions_dict_by_session_id(self, measure_id: int) -> Dict[str, Tuple[Any, Optional[str]]]:
        """以字典形式返回實驗條件（包含單位）"""
        conditions = self.select_conditions_by_session_id(measure_id)
        return {cond['setting_parameters']: (self._coerce_db_value(cond['setting_value']), cond['parameters_unit']) for cond in conditions}
    
    # RawDataFiles 查詢
    def select_rawdata_by_session_id(self,
                                     measure_id: int,
                                     data_type: Optional[str] = None,
                                     session_idx: Optional[int] = None) -> List[Dict[str, Any]]:
        """查詢特定 Measurement (可選 session_idx) 的測量數據"""
        sql = f"""SELECT md.*, ms.measure_id, ms.session_idx
                  FROM {self.TABLE_DATA} md
                  JOIN {self.TABLE_MEASURE_SESSIONS} ms ON md.session_id = ms.session_id
                  WHERE ms.measure_id = ?"""
        params: List[Any] = [measure_id]

        if session_idx is not None:
            sql += " AND ms.session_idx = ?"
            params.append(session_idx)
        if data_type:
            sql += " AND md.data_type = ?"
            params.append(data_type)

        sql += " ORDER BY ms.session_idx, md.data_id"
        return self.query(sql, tuple(params))

    def select_data_ids_paths_by_session(self, session_id: int, data_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """查詢指定 session_id (可選 data_type) 的 data_id 與 file_path。"""
        sql = f"""SELECT data_id, file_path
                  FROM {self.TABLE_DATA}
                  WHERE session_id = ?"""
        params: List[Any] = [session_id]

        if data_type:
            sql += " AND data_type = ?"
            params.append(data_type)

        sql += " ORDER BY data_id"
        rows = self.query(sql, tuple(params))
        return rows

    def select_rawdata_by_data_id(self, data_id: int) -> Optional[Dict[str, Any]]:
        """根據 data_id 查詢單筆測量數據"""
        sql = f"""SELECT md.*, ms.measure_id, ms.session_idx
                  FROM {self.TABLE_DATA} md
                  JOIN {self.TABLE_MEASURE_SESSIONS} ms ON md.session_id = ms.session_id
                  WHERE md.data_id = ?"""
        results = self.query(sql, (data_id,))
        return results[0] if results else None

    # OpticalInfo / ElectricInfo 查詢
    def select_optical_info_by_data_id(self, data_id: int) -> Optional[Dict[str, Any]]:
        """取得單筆數據的光學設定。"""
        rows = self.query(f"SELECT * FROM {self.TABLE_OPTICAL_INFO} WHERE data_id = ?", (data_id,))
        return rows[0] if rows else None

    def select_electric_info_by_data_id(self, data_id: int) -> List[Dict[str, Any]]:
        """取得單筆數據的電性設定（每個 channel 一行）。"""
        return self.query(f"SELECT * FROM {self.TABLE_ELECTRIC_INFO} WHERE data_id = ? ORDER BY channel", (data_id,))

    def select_electric_info_dict_by_data_id(self, data_id: int) -> Dict[str, Dict[str, Any]]:
        """以字典形式返回電性設定，鍵為 channel。"""
        rows = self.select_electric_info_by_data_id(data_id)
        return {row['channel']: {'element': row['element'], 'set_mode': row['set_mode'], 'set_value': row['set_value']} for row in rows}

    def select_another_info_by_data_id(self, data_id: int) -> List[Dict[str, Any]]:
        """查詢自由格式的其它資訊。"""
        return self.query(f"SELECT * FROM {self.TABLE_ANOTHER_INFO} WHERE data_id = ?", (data_id,))

    def select_another_info_dict_by_data_id(self, data_id: int) -> Dict[str, Tuple[Any, Optional[str]]]:
        """以字典形式返回其它資訊（包含單位）。"""
        info = self.select_another_info_by_data_id(data_id)
        return {item['info_key']: (self._coerce_db_value(item['info_value']), item['info_unit']) for item in info}
    
    # Analyses 查詢
    def select_analyses_by_session_id(self,
                                      measure_id: int,
                                      analysis_type: Optional[str] = None,
                                      session_idx: Optional[int] = None) -> List[Dict[str, Any]]:
        """查詢特定 Measurement (可選 session_idx) 的分析執行"""
        sql = f"""SELECT a.*, ms.measure_id, ms.session_idx
                  FROM {self.TABLE_ANALYSES} a
                  JOIN {self.TABLE_MEASURE_SESSIONS} ms ON a.session_id = ms.session_id
                  WHERE ms.measure_id = ?"""
        params: List[Any] = [measure_id]

        if session_idx is not None:
            sql += " AND ms.session_idx = ?"
            params.append(session_idx)
        if analysis_type:
            sql += " AND a.analysis_type = ?"
            params.append(analysis_type)

        sql += " ORDER BY ms.session_idx, a.analysis_id"
        return self.query(sql, tuple(params))

    # AnalysisSources 查詢
    def select_sources_by_analysis_id(self, analysis_id: int) -> List[Dict[str, Any]]:
        """查詢分析所對應的測量數據 ID"""
        return self.query(f"SELECT * FROM {self.TABLE_ANALYSIS_SOURCES} WHERE analysis_id = ?",
                          (analysis_id,))

    def select_sourcesinfo_by_analysis_id(self, analysis_id: int) -> List[Dict[str, Any]]:
        """查詢分析所對應的完整測量數據紀錄"""
        sql = f"""SELECT md.*, ms.measure_id, ms.session_idx
                 FROM {self.TABLE_ANALYSIS_SOURCES} ai
                 JOIN {self.TABLE_DATA} md ON ai.data_id = md.data_id
                 JOIN {self.TABLE_MEASURE_SESSIONS} ms ON md.session_id = ms.session_id
                 WHERE ai.analysis_id = ?
                 ORDER BY md.data_id"""
        return self.query(sql, (analysis_id,))

    def select_analyses_by_data_id(self, data_id: int) -> List[Dict[str, Any]]:
        """查詢某筆測量數據被哪些分析使用"""
        sql = f"""SELECT ar.*, ms.measure_id, ms.session_idx
                  FROM {self.TABLE_ANALYSIS_SOURCES} ai
                  JOIN {self.TABLE_ANALYSES} ar ON ai.analysis_id = ar.analysis_id
                  JOIN {self.TABLE_MEASURE_SESSIONS} ms ON ar.session_id = ms.session_id
                  WHERE ai.data_id = ?
                  ORDER BY ar.analysis_id"""
        return self.query(sql, (data_id,))
    
    # Features 查詢
    def select_features_by_analysis_id(self,analysis_id: int,feature_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """查詢特定分析的特徵"""
        if feature_type:
            return self.query(f"SELECT * FROM {self.TABLE_FEATURES} WHERE analysis_id = ? AND feature_type = ? ORDER BY feature_idx",
                              (analysis_id, feature_type))
        else:
            return self.query(f"SELECT * FROM {self.TABLE_FEATURES} WHERE analysis_id = ? ORDER BY feature_idx",
                              (analysis_id,))
    
    # FeatureMetrics 查詢
    def select_metrics_by_feature_id(self, feature_id: int) -> List[Dict[str, Any]]:
        """查詢特定特徵的所有值"""
        return self.query(f"SELECT * FROM {self.TABLE_METRICS} WHERE feature_id = ?",(feature_id,))
    
    def select_metrics_dict_by_feature_id(self, feature_id: int) -> Dict[str, Tuple[float, Optional[str]]]:
        """以字典形式返回特徵值"""
        values = self.select_metrics_by_feature_id(feature_id)
        return {val['metric_key']: (val['metric_value'], val['metric_unit']) for val in values}
    
    def select_metrics_by_value_range(self,
                                      key: str,
                                      min_value: Optional[float] = None,
                                      max_value: Optional[float] = None,
                                      unit: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        根據特徵值範圍搜索
        
        Args:
            key: 特徵名稱（如 'wavelength'）
            min_value: 最小值（可選）
            max_value: 最大值（可選）
            unit: 單位（可選）
        """
        sql = f"""SELECT metric_id,
                  feature_id,
                  metric_key,
                  metric_value,
                  metric_unit,
                  metric_value AS value,
                  metric_unit AS unit
                  FROM {self.TABLE_METRICS}
                  WHERE metric_key = ?"""
        params = [key]
        
        if unit is not None:
            sql += " AND metric_unit = ?"
            params.append(unit)
        if min_value is not None:
            sql += " AND metric_value >= ?"
            params.append(min_value)
        if max_value is not None:
            sql += " AND metric_value <= ?"
            params.append(max_value)
            
        return self.query(sql, tuple(params))
    
    # 複雜查詢：完整的測量會話資訊
    def select_session_details(self, measure_id: int) -> Dict[str, Any]:
        """
        獲取測量會話的完整資訊（包括 DUT、條件、數據等）
        
        Returns:
            包含所有相關信息的字典
        """
        # 基本會話信息
        session = self.select_session_by_id(measure_id)
        if not session:
            return {}
        
        # DUT 信息
        session['dut'] = self.select_dut_by_id(session['DUT_id'])
        
        # 實驗條件
        session['conditions'] = self.select_conditions_dict_by_session_id(measure_id)
        
        # 測量數據
        session['measurement_data'] = self.select_rawdata_by_session_id(measure_id)
        for data in session['measurement_data']:
            data['optical_info'] = self.select_optical_info_by_data_id(data['data_id'])
            data['electric_info'] = self.select_electric_info_by_data_id(data['data_id'])
            data['another_info'] = self.select_another_info_dict_by_data_id(data['data_id'])
        
        # 分析執行
        analysis_runs = self.select_analyses_by_session_id(measure_id)
        for analysis in analysis_runs:
            analysis['inputs'] = self.select_sourcesinfo_by_analysis_id(analysis['analysis_id'])
            # 每個分析的特徵
            features = self.select_features_by_analysis_id(analysis['analysis_id'])
            for feature in features:
                # 每個特徵的值
                feature['values'] = self.select_metrics_dict_by_feature_id(feature['feature_id'])
            analysis['features'] = features
        session['analysis_runs'] = analysis_runs
        
        return session
    
    # =========================
    # 4. 刪除資料 (DELETE)
    # =========================
    
    def delete_dut(self, dut_id: int) -> int:
        """
        刪除 DUT（會級聯刪除相關的所有數據）
        
        Args:
            dut_id: DUT ID
            
        Returns:
            刪除的行數
        """
        cursor = self.conn.execute("DELETE FROM DUT WHERE DUT_id = ?", (dut_id,))
        self.conn.commit()
        return cursor.rowcount
    
    def delete_session(self, measure_id: int) -> int:
        """
        刪除測量會話（會級聯刪除相關的所有數據）
        
        Args:
            session_id: 會話 ID
            
        Returns:
            刪除的行數
        """
        cursor = self.conn.execute(f"DELETE FROM {self.TABLE_MEASUREMENTS} WHERE measure_id = ?", (measure_id,))
        self.conn.commit()
        return cursor.rowcount
    
    def delete_condition(self, condition_id: int) -> int:
        """刪除實驗條件"""
        cursor = self.conn.execute(f"DELETE FROM {self.TABLE_CONDITIONS} WHERE condition_id = ?", (condition_id,))
        self.conn.commit()
        return cursor.rowcount
    
    def delete_rawdata(self, data_id: int) -> int:
        """刪除測量數據"""
        cursor = self.conn.execute(f"DELETE FROM {self.TABLE_DATA} WHERE data_id = ?", (data_id,))
        self.conn.commit()
        return cursor.rowcount

    def delete_optical_info(self, data_id: int) -> int:
        """刪除光學設定"""
        cursor = self.conn.execute(f"DELETE FROM {self.TABLE_OPTICAL_INFO} WHERE data_id = ?", (data_id,))
        self.conn.commit()
        return cursor.rowcount

    def delete_electric_info(self, data_id: int, channel: Optional[str] = None) -> int:
        """刪除電性設定（可選擇性僅刪除某 channel）。"""
        if channel is None:
            cursor = self.conn.execute(f"DELETE FROM {self.TABLE_ELECTRIC_INFO} WHERE data_id = ?", (data_id,))
        else:
            cursor = self.conn.execute(f"DELETE FROM {self.TABLE_ELECTRIC_INFO} WHERE data_id = ? AND channel = ?",
                                       (data_id, channel))
        self.conn.commit()
        return cursor.rowcount

    def delete_another_info(self, data_id: int, info_key: Optional[str] = None, info_unit: Optional[str] = None) -> int:
        """刪除其他資訊，可依 key/unit 篩選。"""
        sql = f"DELETE FROM {self.TABLE_ANOTHER_INFO} WHERE data_id = ?"
        params: List[Any] = [data_id]
        if info_key is not None:
            sql += " AND info_key = ?"
            params.append(info_key)
        if info_unit is not None:
            sql += " AND info_unit = ?"
            params.append(info_unit)
        cursor = self.conn.execute(sql, tuple(params))
        self.conn.commit()
        return cursor.rowcount
    
    def delete_analyses(self, analysis_id: int) -> int:
        """刪除分析執行（會級聯刪除相關特徵和特徵值）"""
        cursor = self.conn.execute(f"DELETE FROM {self.TABLE_ANALYSES} WHERE analysis_id = ?", (analysis_id,))
        self.conn.commit()
        return cursor.rowcount

    def delete_sources(self, analysis_id: int, data_id: int) -> int:
        """刪除分析與測量數據的對應"""
        cursor = self.conn.execute(f"DELETE FROM {self.TABLE_ANALYSIS_SOURCES} WHERE analysis_id = ? AND data_id = ?",
                                   (analysis_id, data_id))
        self.conn.commit()
        return cursor.rowcount
    
    def delete_feature(self, feature_id: int) -> int:
        """刪除分析特徵（會級聯刪除相關特徵值）"""
        cursor = self.conn.execute(f"DELETE FROM {self.TABLE_FEATURES} WHERE feature_id = ?", (feature_id,))
        self.conn.commit()
        return cursor.rowcount
    
    def delete_metrics(self, metric_id: int) -> int:
        """刪除特徵值"""
        cursor = self.conn.execute(f"DELETE FROM {self.TABLE_METRICS} WHERE metric_id = ?", (metric_id,))
        self.conn.commit()
        return cursor.rowcount
    
    # 批量刪除
    def delete_sessions_by_dut(self, dut_id: int) -> int:
        """刪除特定 DUT 的所有會話"""
        cursor = self.conn.execute(f"DELETE FROM {self.TABLE_MEASUREMENTS} WHERE DUT_id = ?", (dut_id,))
        self.conn.commit()
        return cursor.rowcount
    
    def delete_old_sessions(self, before_date: datetime) -> int:
        """刪除指定日期之前的所有會話"""
        cursor = self.conn.execute(f"DELETE FROM {self.TABLE_MEASUREMENTS} WHERE measured_at < ?",
                       (before_date,))
        self.conn.commit()
        return cursor.rowcount
    
    # =========================
    # 輔助方法
    # =========================
    
    def get_table_count(self, table_name: str) -> int:
        """獲取表的記錄數"""
        cursor = self.conn.execute(f"SELECT COUNT(*) as count FROM {table_name}")
        return cursor.fetchone()['count']
    
    def get_database_stats(self) -> Dict[str, int]:
        """獲取資料庫統計信息"""
        tables = [self.TABLE_DUT, 
                  self.TABLE_MEASUREMENTS, 
                  self.TABLE_MEASURE_SESSIONS, 
                  self.TABLE_CONDITIONS, 
                  self.TABLE_DATA, 
                  self.TABLE_OPTICAL_INFO,
                  self.TABLE_ELECTRIC_INFO,
                  self.TABLE_ANOTHER_INFO,
                  self.TABLE_ANALYSES, 
                  self.TABLE_ANALYSIS_SOURCES, 
                  self.TABLE_FEATURES, 
                  self.TABLE_METRICS]
        return {table: self.get_table_count(table) for table in tables}

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

        return valid_files, invalid_files

    @classmethod
    def move_file(cls, src_dst):
        src, dst = src_dst
        try:
            src.rename(dst)  # 同磁碟機非常快
        except OSError:
            shutil.move(str(src), str(dst))  # 跨磁碟機降級

    @classmethod
    def copy_file(cls, src_dst):
        """Copy a file to destination; uses copy2 to preserve metadata."""
        src, dst = src_dst
        shutil.copy2(src, dst)

    def import_from_measurement_folder(self, folder_path, schema_file="schema.sql"):
        """批次匯入資料夾中的測量檔案並寫入資料庫與 RawDataFiles 目錄。

        Args:
            folder_path: 含有測量檔案的資料夾路徑。
            schema_file: 在需要時用來初始化資料庫的 schema 檔案。

        Returns:
            None
        """

        folder = Path(folder_path)
        # 先解析資料夾，拆出合規檔案與命名失敗清單
        valid_files, invalid_files = self.parse_folder(folder)
        target_root_path = Path(self.db_path).parent / "RawDataFiles"

        try:
            # 若資料庫尚未建立則依 schema 初始化
            self.create_db(schema_file)
        except Exception:
            pass

        tested_timestamp = folder.stat().st_mtime
        move_path = []
        try:
            # 使用交易確保整批匯入原子性
            self.conn.execute("BEGIN")
            for filepath, file_info_raw in tqdm(valid_files.items(), desc="Importing", unit="file"):
                file_info = dict(file_info_raw)
                session_name = folder.name
                repeat_folder = r"#" + file_info["repeat"]
                session_idx = int(file_info["repeat"])
                # 依檔名資訊建立資料儲存層級
                target_dir = (target_root_path/ 
                              file_info["wafer"]/ 
                              file_info["doe"]/ 
                              file_info["cage"]/
                              file_info["device"]/
                              f"die{file_info['die']}"/ session_name/ repeat_folder)
                target_dir.mkdir(parents=True, exist_ok=True)
                dst = target_dir / filepath.name

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

                data_id = self.insert_rawdata_file(session_id=session_id,
                                                   data_type=file_info["datatype"],
                                                   recorded_at=filepath.stat().st_mtime,
                                                   file_path=str(dst),
                                                   file_name=filepath.name,
                                                   commit=False)
                smu_entries = file_info.pop("SMU")
                arguments = file_info.pop("arguments")

                wavelength_start=None
                wavelength_stop=None
                sweep_rate=None

                if file_info["datatype"] in ["SPCM"]:
                    head, _ = read_spectrum(filepath)
                    wavelength_start=head["WavelengthStart"]
                    wavelength_stop=head["WavelengthStop"]
                    sweep_rate=head["SweepRate"]

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
            # 移動或複製檔案至目標資料夾
            #for src, dst in tqdm(move_path, desc="Moving", unit="file"):
                #shutil.move(str(src), str(dst))
                #shutil.copy2(str(src), str(dst))
            #多線程-移動或複製檔案至目標資料夾
            with ThreadPoolExecutor() as executor:
                # list(tqdm(executor.map(self.move_file, move_path), 
                #         total=len(move_path), 
                #         desc="Moving", 
                #         unit="file"))
                list(tqdm(executor.map(self.copy_file, move_path), 
                        total=len(move_path), 
                        desc="Moving", 
                        unit="file"))
        except Exception:
            if self.conn:
                self.conn.rollback()
            raise
        print(f"匯入資料庫完成")
 
