import sqlite3
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path


class DatabaseAPI:
    """基於 SQLite 的資料庫 API，用於管理光譜測量數據"""
    
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
    
    # =========================
    # 1. 創建資料庫
    # =========================
    
    def create_database(self, schema_file: str = "schema.sql"):
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
        
    def reset_database(self, schema_file: str = "schema.sql"):
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
        self.create_database(schema_file)
    
    # =========================
    # 2. 寫入資料 (INSERT/UPDATE)
    # =========================
    
    # DUT 表
    def insert_dut(self, wafer: str, doe: str, die: int, cage: str, device: str) -> int:
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
        cursor = self.conn.execute("""INSERT OR IGNORE INTO DUT 
                                   (wafer, DOE, die, cage, device) 
                                   VALUES (?, ?, ?, ?, ?)""",
                                   (wafer, doe, die, cage, device))
        self.conn.commit()
        if cursor.rowcount == 0:
            cursor = self.conn.execute(
                """SELECT DUT_id FROM DUT WHERE wafer = ? AND DOE = ? AND die = ? AND cage = ? AND device = ?""",
                (wafer, doe, die, cage, device)
            )
            row = cursor.fetchone()
            return row['DUT_id'] if row else 0
        return cursor.lastrowid
    
    # MeasurementSessions 表
    def insert_measurement_session(self, 
                                   dut_id: int,
                                   session_name: Optional[str] = None,
                                   measurement_datetime: Optional[datetime] = None,
                                   operator: Optional[str] = None,
                                   system_version: Optional[str] = None,
                                   notes: Optional[str] = None) -> int:
        """
        插入測量會話記錄
        
        Args:
            dut_id: DUT ID
            session_name: 會話名稱
            measurement_datetime: 測量時間（默認為當前時間）
            operator: 操作員
            system_version: 系統版本
            notes: 備註
            
        Returns:
            session_id
        """
        if measurement_datetime is None:
            measurement_datetime = datetime.now().replace(microsecond=0)
            
        cursor = self.conn.execute("""INSERT INTO MeasurementSessions 
                                   (DUT_id, session_name, measurement_datetime, operator, system_version, notes)
                                   VALUES (?, ?, ?, ?, ?, ?)""",
                                   (dut_id, session_name, measurement_datetime, operator, system_version, notes))
        self.conn.commit()
        return cursor.lastrowid
    
    # ExperimentalConditions 表
    def insert_experimental_conditions(self, session_id: int, conditions: Dict[str, Any]):
        """
        批量插入實驗條件
        
        Args:
            session_id: 會話 ID
            conditions: 條件字典 {'temperature': 25.0, 'voltage': 3.3, ...}
                       或包含單位的字典 {'temperature': (25.0, '°C'), 'voltage': (3.3, 'V'), ...}
        """
        for key, value_data in conditions.items():
            # 支援兩種格式：純數值 或 (數值, 單位) 元組
            if isinstance(value_data, (tuple, list)) and len(value_data) == 2:
                value, unit = value_data
            else:
                value, unit = value_data, None
            
            self.conn.execute("""INSERT OR IGNORE INTO ExperimentalConditions 
                              (session_id, key, value, unit)
                              VALUES (?, ?, ?, ?)""",
                              (session_id, key, value, unit))
        self.conn.commit()
    
    # MeasurementData 表
    def insert_measurement_data(self,
                                session_id: int,
                                data_type: str,
                                file_path: str,
                                created_time: Optional[datetime] = None) -> int:
        """
        插入測量數據記錄
        
        Args:
            session_id: 會話 ID
            data_type: 數據類型（如 'spectrum'）
            file_path: 文件路徑
            created_time: 創建時間（默認為當前時間）
            
        Returns:
            data_id
        """
        if created_time is None:
            created_time = datetime.now().replace(microsecond=0)
            
        cursor = self.conn.execute("""INSERT INTO MeasurementData 
                                   (session_id, data_type, file_path, created_time)
                                   VALUES (?, ?, ?, ?)""",
                                   (session_id, data_type, file_path, created_time))
        self.conn.commit()
        return cursor.lastrowid

    # DataInfo 表
    def insert_data_info(self, data_id: int, info: Dict[str, Any]):
        """
        批量插入測量數據資訊

        Args:
            data_id: 測量數據 ID
            info: 資訊字典 {'exposure': 1.2, 'gain': 10.0, ...}
                  或包含單位的字典 {'exposure': (1.2, 's'), 'gain': (10.0, 'dB'), ...}
        """
        for key, value_data in info.items():
            if isinstance(value_data, (tuple, list)) and len(value_data) == 2:
                value, unit = value_data
            else:
                value, unit = value_data, None

            self.conn.execute("""INSERT OR IGNORE INTO DataInfo 
                              (data_id, key, value, unit)
                              VALUES (?, ?, ?, ?)""",
                              (data_id, key, value, unit))
        self.conn.commit()
    
    # AnalysisRuns 表
    def insert_analysis_run(self,session_id: int,analysis_type: str,created_time: Optional[datetime] = None) -> int:
        """
        插入分析執行記錄
        
        Args:
            session_id: 會話 ID
            analysis_type: 分析類型（如 'peak_detection'）
            created_time: 創建時間（默認為當前時間）
            
        Returns:
            analysis_id
        """
        if created_time is None:
            created_time = datetime.now().replace(microsecond=0)
            
        cursor = self.conn.execute("""INSERT INTO AnalysisRuns 
                                   (session_id, analysis_type, created_time)
                                   VALUES (?, ?, ?)""",
                                   (session_id, analysis_type, created_time))
        self.conn.commit()
        return cursor.lastrowid
    
    # AnalysisFeatures 表
    def insert_analysis_feature(self,analysis_id: int,feature_type: str,feature_index: int) -> int:
        """
        插入分析特徵記錄
        
        Args:
            analysis_id: 分析 ID
            feature_type: 特徵類型（如 'peak', 'valley'）
            feature_index: 特徵索引
            
        Returns:
            feature_id
        """
        cursor = self.conn.execute("""INSERT INTO AnalysisFeatures 
                                   (analysis_id, feature_type, feature_index)
                                   VALUES (?, ?, ?)""",
                                   (analysis_id, feature_type, feature_index))
        self.conn.commit()
        return cursor.lastrowid
    
    # FeatureValues 表
    def insert_feature_values(self, feature_id: int, values: Dict[str, Any]):
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
            
            self.conn.execute("""INSERT OR IGNORE INTO FeatureValues 
                              (feature_id, key, value, unit)
                              VALUES (?, ?, ?, ?)""",
                              (feature_id, key, value, unit))
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
    def get_dut_by_id(self, dut_id: int) -> Optional[Dict[str, Any]]:
        """根據 ID 查詢 DUT"""
        results = self.query("SELECT * FROM DUT WHERE DUT_id = ?", (dut_id,))
        return results[0] if results else None
    
    def get_duts(self, wafer: Optional[str] = None, die: Optional[int] = None) -> List[Dict[str, Any]]:
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
    
    # MeasurementSessions 查詢
    def get_session_by_id(self, session_id: int) -> Optional[Dict[str, Any]]:
        """根據 ID 查詢測量會話"""
        results = self.query("SELECT * FROM MeasurementSessions WHERE session_id = ?", (session_id,))
        return results[0] if results else None
    
    def get_sessions_by_dut(self, dut_id: int) -> List[Dict[str, Any]]:
        """查詢特定 DUT 的所有測量會話"""
        return self.query("SELECT * FROM MeasurementSessions WHERE DUT_id = ? ORDER BY measurement_datetime DESC",(dut_id,))
    
    def get_sessions_by_date_range(self,start_date: datetime,end_date: datetime) -> List[Dict[str, Any]]:
        """查詢日期範圍內的測量會話"""
        return self.query("""SELECT * FROM MeasurementSessions 
                          WHERE measurement_datetime BETWEEN ? AND ? ORDER BY measurement_datetime DESC""",
                          (start_date, end_date))
    
    # ExperimentalConditions 查詢
    def get_conditions_by_session(self, session_id: int) -> List[Dict[str, Any]]:
        """查詢特定會話的所有實驗條件"""
        return self.query("SELECT * FROM ExperimentalConditions WHERE session_id = ?",(session_id,))
    
    def get_conditions_dict(self, session_id: int) -> Dict[str, Tuple[float, Optional[str]]]:
        """以字典形式返回實驗條件（包含單位）"""
        conditions = self.get_conditions_by_session(session_id)
        return {cond['key']: (cond['value'], cond['unit']) for cond in conditions}
    
    # MeasurementData 查詢
    def get_measurement_data_by_session(self,session_id: int,data_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """查詢特定會話的測量數據"""
        if data_type:
            return self.query("SELECT * FROM MeasurementData WHERE session_id = ? AND data_type = ?",
                              (session_id, data_type))
        else:
            return self.query("SELECT * FROM MeasurementData WHERE session_id = ?",
                              (session_id,))

    # DataInfo 查詢
    def get_data_info_by_data(self, data_id: int) -> List[Dict[str, Any]]:
        """查詢特定測量數據的所有資訊"""
        return self.query("SELECT * FROM DataInfo WHERE data_id = ?", (data_id,))

    def get_data_info_dict(self, data_id: int) -> Dict[str, Tuple[float, Optional[str]]]:
        """以字典形式返回測量數據資訊（包含單位）"""
        info = self.get_data_info_by_data(data_id)
        return {item['key']: (item['value'], item['unit']) for item in info}
    
    # AnalysisRuns 查詢
    def get_analysis_runs_by_session(self,session_id: int,analysis_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """查詢特定會話的分析執行"""
        if analysis_type:
            return self.query("SELECT * FROM AnalysisRuns WHERE session_id = ? AND analysis_type = ?",
                              (session_id, analysis_type))
        else:
            return self.query("SELECT * FROM AnalysisRuns WHERE session_id = ?",
                              (session_id,))
    
    # AnalysisFeatures 查詢
    def get_features_by_analysis(self,analysis_id: int,feature_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """查詢特定分析的特徵"""
        if feature_type:
            return self.query("SELECT * FROM AnalysisFeatures WHERE analysis_id = ? AND feature_type = ? ORDER BY feature_index",
                              (analysis_id, feature_type))
        else:
            return self.query("SELECT * FROM AnalysisFeatures WHERE analysis_id = ? ORDER BY feature_index",
                              (analysis_id,))
    
    # FeatureValues 查詢
    def get_feature_values(self, feature_id: int) -> List[Dict[str, Any]]:
        """查詢特定特徵的所有值"""
        return self.query("SELECT * FROM FeatureValues WHERE feature_id = ?",(feature_id,))
    
    def get_feature_values_dict(self, feature_id: int) -> Dict[str, Tuple[float, Optional[str]]]:
        """以字典形式返回特徵值"""
        values = self.get_feature_values(feature_id)
        return {val['key']: (val['value'], val['unit']) for val in values}
    
    def search_features_by_value(self,
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
        sql = "SELECT * FROM FeatureValues WHERE key = ?"
        params = [key]
        
        if unit is not None:
            sql += " AND unit = ?"
            params.append(unit)
        if min_value is not None:
            sql += " AND value >= ?"
            params.append(min_value)
        if max_value is not None:
            sql += " AND value <= ?"
            params.append(max_value)
            
        return self.query(sql, tuple(params))
    
    # 複雜查詢：完整的測量會話資訊
    def get_session_full_info(self, session_id: int) -> Dict[str, Any]:
        """
        獲取測量會話的完整資訊（包括 DUT、條件、數據等）
        
        Returns:
            包含所有相關信息的字典
        """
        # 基本會話信息
        session = self.get_session_by_id(session_id)
        if not session:
            return {}
        
        # DUT 信息
        session['dut'] = self.get_dut_by_id(session['DUT_id'])
        
        # 實驗條件
        session['conditions'] = self.get_conditions_dict(session_id)
        
        # 測量數據
        session['measurement_data'] = self.get_measurement_data_by_session(session_id)
        
        # 分析執行
        analysis_runs = self.get_analysis_runs_by_session(session_id)
        for analysis in analysis_runs:
            # 每個分析的特徵
            features = self.get_features_by_analysis(analysis['analysis_id'])
            for feature in features:
                # 每個特徵的值
                feature['values'] = self.get_feature_values_dict(feature['feature_id'])
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
    
    def delete_session(self, session_id: int) -> int:
        """
        刪除測量會話（會級聯刪除相關的所有數據）
        
        Args:
            session_id: 會話 ID
            
        Returns:
            刪除的行數
        """
        cursor = self.conn.execute("DELETE FROM MeasurementSessions WHERE session_id = ?", (session_id,))
        self.conn.commit()
        return cursor.rowcount
    
    def delete_experimental_condition(self, condition_id: int) -> int:
        """刪除實驗條件"""
        cursor = self.conn.execute("DELETE FROM ExperimentalConditions WHERE condition_id = ?", (condition_id,))
        self.conn.commit()
        return cursor.rowcount
    
    def delete_measurement_data(self, data_id: int) -> int:
        """刪除測量數據"""
        cursor = self.conn.execute("DELETE FROM MeasurementData WHERE data_id = ?", (data_id,))
        self.conn.commit()
        return cursor.rowcount

    def delete_data_info(self, info_id: int) -> int:
        """刪除測量數據資訊"""
        cursor = self.conn.execute("DELETE FROM DataInfo WHERE info_id = ?", (info_id,))
        self.conn.commit()
        return cursor.rowcount
    
    def delete_analysis_run(self, analysis_id: int) -> int:
        """刪除分析執行（會級聯刪除相關特徵和特徵值）"""
        cursor = self.conn.execute("DELETE FROM AnalysisRuns WHERE analysis_id = ?", (analysis_id,))
        self.conn.commit()
        return cursor.rowcount
    
    def delete_analysis_feature(self, feature_id: int) -> int:
        """刪除分析特徵（會級聯刪除相關特徵值）"""
        cursor = self.conn.execute("DELETE FROM AnalysisFeatures WHERE feature_id = ?", (feature_id,))
        self.conn.commit()
        return cursor.rowcount
    
    def delete_feature_value(self, value_id: int) -> int:
        """刪除特徵值"""
        cursor = self.conn.execute("DELETE FROM FeatureValues WHERE value_id = ?", (value_id,))
        self.conn.commit()
        return cursor.rowcount
    
    # 批量刪除
    def delete_sessions_by_dut(self, dut_id: int) -> int:
        """刪除特定 DUT 的所有會話"""
        cursor = self.conn.execute("DELETE FROM MeasurementSessions WHERE DUT_id = ?", (dut_id,))
        self.conn.commit()
        return cursor.rowcount
    
    def delete_old_sessions(self, before_date: datetime) -> int:
        """刪除指定日期之前的所有會話"""
        cursor = self.conn.execute("DELETE FROM MeasurementSessions WHERE measurement_datetime < ?",
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
        tables = ['DUT', 'MeasurementSessions', 'ExperimentalConditions', 
                  'MeasurementData', 'DataInfo', 'AnalysisRuns', 'AnalysisFeatures', 'FeatureValues']
        return {table: self.get_table_count(table) for table in tables}

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
