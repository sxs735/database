# SQLite 資料庫 API

基於 schema.sql 的 Python SQLite 資料庫 API，用於管理光譜測量數據。

## 功能特性

✅ **創建資料庫** - 根據 schema.sql 自動創建資料庫結構  
✅ **寫入資料** - 提供完整的 INSERT 方法，支援所有表格  
✅ **查詢資料** - 提供靈活的查詢方法，支援簡單和複雜查詢  
✅ **刪除資料** - 支援單筆和批量刪除，自動處理級聯刪除  

## 快速開始

### 1. 創建資料庫

```python
from database_api import DatabaseAPI

# 創建新資料庫
db = DatabaseAPI("my_database.db")
db.create_database("schema.sql")
db.close()
```

### 2. 插入數據

```python
with DatabaseAPI("my_database.db") as db:
    # 插入 DUT
    dut_id = db.insert_dut(
        wafer="W001",
        die=1,
        cage="C1",
        device="D001"
    )
    
    # 插入測量會話
    session_id = db.insert_measurement_session(
        dut_id=dut_id,
        session_name="光譜測量_2026_01_30",
        operator="張三",
        system_version="v1.0"
    )
    
    # 插入實驗條件
    db.insert_experimental_conditions(session_id, {
        'temperature': 25.0,
        'voltage': 3.3,
        'current': 0.1
    })
    
    # 插入測量數據
    data_id = db.insert_measurement_data(
        session_id=session_id,
        data_type="spectrum",
        file_path="/data/spectrum_001.csv"
    )
```

### 3. 查詢數據

```python
with DatabaseAPI("my_database.db") as db:
    # 查詢所有 DUT
    duts = db.get_duts()
    
    # 查詢特定 DUT 的測量會話
    sessions = db.get_sessions_by_dut(dut_id=1)
    
    # 獲取完整的會話資訊（包括 DUT、條件、數據、分析結果）
    full_info = db.get_session_full_info(session_id=1)
    
    # 搜索特定範圍的特徵
    features = db.search_features_by_value(
        key='wavelength',
        min_value=1550.0,
        max_value=1560.0
    )
```

### 4. 刪除數據

```python
with DatabaseAPI("my_database.db") as db:
    # 刪除會話（會級聯刪除相關的所有數據）
    db.delete_session(session_id=1)
    
    # 刪除 DUT（會級聯刪除相關的所有數據）
    db.delete_dut(dut_id=1)
    
    # 刪除舊的會話
    from datetime import datetime, timedelta
    one_week_ago = datetime.now() - timedelta(days=7)
    db.delete_old_sessions(one_week_ago)
```

## API 文檔

### 初始化

```python
db = DatabaseAPI(db_path="database.db")
```

使用 context manager（推薦）：
```python
with DatabaseAPI("database.db") as db:
    # 進行操作
    pass
```

### 創建資料庫

| 方法 | 說明 |
|------|------|
| `create_database(schema_file)` | 根據 schema 文件創建資料庫 |
| `reset_database(schema_file)` | 重置資料庫（刪除並重新創建） |

### 寫入資料

#### DUT 表
```python
dut_id = db.insert_dut(wafer, die, cage, device)  # 重複會自動忽略
```

#### MeasurementSessions 表
```python
session_id = db.insert_measurement_session(
    dut_id,
    session_name=None,
    measurement_datetime=None,  # 默認當前時間
    operator=None,
    system_version=None,
    notes=None
)
```

#### ExperimentalConditions 表
```python
# 批量插入（支援兩種格式）
# 格式 1：純數值
db.insert_experimental_conditions(session_id, {
    'temperature': 25.0,
    'voltage': 3.3
})

# 格式 2：(數值, 單位) 元組
db.insert_experimental_conditions(session_id, {
    'temperature': (25.0, '°C'),
    'voltage': (3.3, 'V')
})
```

#### MeasurementData 表
```python
data_id = db.insert_measurement_data(
    session_id,
    data_type,
    file_path,
    created_time=None  # 默認當前時間
)
```

#### DataInfo 表
```python
# 批量插入（支援兩種格式）
# 格式 1：純數值
db.insert_data_info(data_id, {
    'exposure': 1.2,
    'gain': 10.0
})

# 格式 2：(數值, 單位) 元組
db.insert_data_info(data_id, {
    'exposure': (1.2, 's'),
    'gain': (10.0, 'dB')
})
```

#### AnalysisRuns 表
```python
analysis_id = db.insert_analysis_run(
    session_id,
    analysis_type,
    created_time=None
)
```

#### AnalysisFeatures 表
```python
feature_id = db.insert_analysis_feature(
    analysis_id,
    feature_type,  # 'peak', 'valley'
    feature_index
)
```

#### FeatureValues 表
```python
# 批量插入（支援兩種格式）
# 格式 1：純數值
db.insert_feature_values(feature_id, {
    'wavelength': 1550.0,
    'intensity': 100.0,
    'fwhm': 2.5
})

# 格式 2：(數值, 單位) 元組
db.insert_feature_values(feature_id, {
    'wavelength': (1550.0, 'nm'),
    'intensity': (100.0, 'dBm'),
    'fwhm': (2.5, 'nm')
})
```
```

### 查詢資料

#### 基本查詢
```python
# DUT
dut = db.get_dut_by_id(dut_id)
duts = db.get_duts(wafer=None, die=None)

# MeasurementSessions
session = db.get_session_by_id(session_id)
sessions = db.get_sessions_by_dut(dut_id)
sessions = db.get_sessions_by_date_range(start_date, end_date)

# ExperimentalConditions
conditions = db.get_conditions_by_session(session_id)
conditions_dict = db.get_conditions_dict(session_id)  # 返回 {key: (value, unit), ...}

# MeasurementData
data = db.get_measurement_data_by_session(session_id, data_type=None)

# DataInfo
info = db.get_data_info_by_data(data_id)
info_dict = db.get_data_info_dict(data_id)

# AnalysisRuns
runs = db.get_analysis_runs_by_session(session_id, analysis_type=None)

# AnalysisFeatures
features = db.get_features_by_analysis(analysis_id, feature_type=None)

# FeatureValues
values = db.get_feature_values(feature_id)
values_dict = db.get_feature_values_dict(feature_id)
features = db.search_features_by_value(key, min_value=None, max_value=None, unit=None)
```

#### 複雜查詢
```python
# 獲取完整的會話資訊
full_info = db.get_session_full_info(session_id)

# 自定義 SQL 查詢
results = db.query(sql, params=())
```

### 刪除資料

```python
# 刪除單筆記錄
db.delete_dut(dut_id)
db.delete_session(session_id)
db.delete_experimental_condition(condition_id)
db.delete_measurement_data(data_id)
db.delete_data_info(info_id)
db.delete_analysis_run(analysis_id)
db.delete_analysis_feature(feature_id)
db.delete_feature_value(value_id)

# 批量刪除
db.delete_sessions_by_dut(dut_id)
db.delete_old_sessions(before_date)
```

### 輔助方法

```python
# 獲取表的記錄數
count = db.get_table_count(table_name)

# 獲取資料庫統計信息
stats = db.get_database_stats()
# 返回: {'DUT': 10, 'MeasurementSessions': 25, ...}

# 匯出整個資料庫為 xlsx
output_path = db.export_all_tables_to_xlsx("database_export.xlsx")
print(f"已輸出: {output_path}")
```

> 匯出功能需要安裝 pandas 與 openpyxl

## 數據結構說明

### 資料庫架構

```
DUT (晶片資訊)
└── MeasurementSessions (測量會話)
    ├── ExperimentalConditions (實驗條件)
    ├── MeasurementData (測量數據文件)
    │   └── DataInfo (測量數據資訊)
    └── AnalysisRuns (分析執行)
        └── AnalysisFeatures (分析特徵)
            └── FeatureValues (特徵值)
```

### 級聯刪除

當刪除上層記錄時，會自動刪除相關的下層記錄：
- 刪除 DUT → 刪除所有相關的測量會話及其子數據
- 刪除 MeasurementSession → 刪除所有相關的條件、數據、數據資訊、分析結果
- 刪除 AnalysisRun → 刪除所有相關的特徵和特徵值
- 刪除 AnalysisFeature → 刪除所有相關的特徵值

## 完整範例

請參考 `example_usage.py` 文件，包含以下範例：

1. 創建資料庫
2. 插入基本數據
3. 插入分析數據（峰值檢測）
4. 查詢數據
5. 查詢完整會話資訊
6. 搜索特徵
7. 刪除數據
8. 批量插入數據
9. 自定義查詢

執行範例：
```bash
python example_usage.py
```

## 批次匯入資料

### 檔名格式

批次匯入工具支援以下檔名格式：
```
{datatype}_{wafer}_die{die_number}_{cage}_{device}_{temperature}C_ch_{channel_in}_{channel_out}_{power}dBm_pn_{voltage}mV_heat_{heat_voltage}_mV.csv
```

**範例：**
```
spectrum_W001_die1_C1_D001_25C_ch_1_2_10dBm_pn_3300mV_heat_0_mV.csv
```

### 使用批次匯入

```bash
# 匯入資料夾中的所有 CSV 檔案
python batch_import.py ./data_folder

# 指定自訂資料庫路徑
python batch_import.py ./data_folder my_database.db
```

### 批次匯入流程

1. **掃描資料夾**：找出所有 CSV 檔案
2. **解析檔名**：從檔名提取 DUT 資訊、實驗條件等
3. **創建 DUT**：檢查是否已存在，不存在則創建
4. **創建會話**：為每個 CSV 檔案創建一個測量會話
5. **插入數據**：存儲實驗條件、測量數據資訊、CSV 檔案路徑
6. **統計輸出**：顯示匯入結果統計

### 檔名參數說明

| 參數 | 說明 | 單位 |
|------|------|------|
| datatype | 數據類型（如 spectrum、power） | - |
| wafer | 晶圓編號 | - |
| die_number | 晶粒編號 | - |
| cage | 籠編號 | - |
| device | 裝置編號 | - |
| temperature | 測量溫度 | °C |
| channel_in | 輸入通道 | - |
| channel_out | 輸出通道 | - |
| power | 輸入功率 | dBm |
| voltage | 工作電壓 | mV |
| heat_voltage | 加熱電壓 | mV |

## 注意事項

1. **外鍵約束**：資料庫啟用了外鍵約束（`PRAGMA foreign_keys = ON`），確保數據完整性
2. **級聯刪除**：刪除父表記錄時會自動刪除子表記錄
3. **Context Manager**：建議使用 `with` 語句管理資料庫連接
4. **日期時間**：日期時間參數接受 Python `datetime` 對象
5. **唯一約束**：某些表有唯一約束（如 DUT 的 wafer+die+cage+device 組合）

## 錯誤處理

```python
try:
    with DatabaseAPI("database.db") as db:
        # 進行操作
        dut_id = db.insert_dut("W001", "D001", 1, "C1", "D001")
except sqlite3.IntegrityError as e:
    print(f"外鍵約束或其他完整性約束違反: {e}")
except sqlite3.OperationalError as e:
    print(f"操作錯誤: {e}")
except FileNotFoundError as e:
    print(f"文件不存在: {e}")
```

### 約束衝突行為

由於使用 `ON CONFLICT IGNORE`，以下情況**不會**產生錯誤：
- DUT 重複插入（wafer+DOE+die+cage+device 相同）
- ExperimentalConditions 重複插入（session_id+key+unit 相同）
- MeasurementData 重複插入（file_path 相同）
- DataInfo 重複插入（data_id+key+unit 相同）
- AnalysisFeatures 重複插入（analysis_id+feature_type+feature_index 相同）
- FeatureValues 重複插入（feature_id+key+unit 相同）

但以下情況**仍會**產生 `sqlite3.IntegrityError`：
- FOREIGN KEY 約束冲突（例如 session_id 不存在）
- NOT NULL 約束冑反（應通過代碼參數驗證）

## 文件說明

- `database_api.py` - 主要的 API 類
- `example_usage.py` - 使用範例
- `schema.sql` - 資料庫結構定義
- `README.md` - 本說明文件

## 系統需求

- Python 3.7+
- SQLite3（Python 內建）

無需安裝額外的依賴包。
