"""
資料庫 API 使用範例
示範如何使用 DatabaseAPI 進行各種操作
"""

from database_api import DatabaseAPI
from datetime import datetime, timedelta


def example_1_create_database():
    """範例 1: 創建資料庫"""
    print("=" * 50)
    print("範例 1: 創建資料庫")
    print("=" * 50)
    
    db = DatabaseAPI("example.db")
    db.create_database("schema.sql")
    db.close()
    
    print("✓ 資料庫創建完成\n")


def example_2_insert_basic_data():
    """範例 2: 插入基本數據"""
    print("=" * 50)
    print("範例 2: 插入基本數據")
    print("=" * 50)
    
    with DatabaseAPI("example.db") as db:
        # 插入 DUT
        dut_id = db.insert_dut(wafer="W001",
                               doe="D001",
                               die=1,
                               cage="C1",
                               device="D001")
        print(f"✓ 插入 DUT，ID: {dut_id}")
        
        # 插入測量會話
        session_id = db.insert_measurement_session(dut_id=dut_id,
                                                   session_name="光譜測量_2026_01_30",
                                                   operator="張三",
                                                   system_version="v1.0",
                                                   notes="第一次測量")
        print(f"✓ 插入測量會話，ID: {session_id}")
        
        # 插入實驗條件（支援無單位和有單位兩種格式）
        db.insert_experimental_conditions(session_id, {'temperature': (25.0, '°C'),
                                                       'voltage': (3.3, 'V'),
                                                       'current': (0.1, 'A')})
        print(f"✓ 插入實驗條件")
        
        # 插入測量數據
        data_id = db.insert_measurement_data(session_id=session_id,
                                             data_type="spectrum",
                                             file_path="/data/spectrum_001.csv")
        print(f"✓ 插入測量數據，ID: {data_id}\n")

        # 插入測量數據資訊（支援無單位和有單位兩種格式）
        db.insert_data_info(data_id, {
            'exposure': (1.2, 's'),
            'gain': (10.0, 'dB')
        })
        print("✓ 插入測量數據資訊")


def example_3_insert_analysis_data():
    """範例 3: 插入分析數據（峰值檢測結果）"""
    print("=" * 50)
    print("範例 3: 插入分析數據")
    print("=" * 50)
    
    with DatabaseAPI("example.db") as db:
        # 假設我們已經有一個 session_id = 1
        session_id = 1
        
        # 創建分析執行
        analysis_id = db.insert_analysis_run(session_id=session_id,
                                             analysis_type="peak_detection")
        print(f"✓ 插入分析執行，ID: {analysis_id}")

        # 將測量數據綁定為分析輸入
        data_list = db.get_measurement_data_by_session(session_id)
        if data_list:
            data_ids = [item['data_id'] for item in data_list]
            db.insert_analysis_inputs(analysis_id, data_ids)
            print(f"✓ 綁定 {len(data_ids)} 筆測量數據為分析輸入")
        
        # 插入檢測到的峰值
        for i in range(3):  # 假設檢測到 3 個峰值
            feature_id = db.insert_analysis_feature(analysis_id=analysis_id,
                                                    feature_type="peak",
                                                    feature_index=i)
            
            # 為每個峰值插入特徵值（支援無單位和有單位兩種格式）
            db.insert_feature_values(feature_id, {'wavelength': (1550.0 + i * 10, 'nm'),
                                                  'intensity': (100.0 - i * 5, 'dBm'),
                                                  'fwhm': (2.5 + i * 0.5, 'nm')})
            print(f"✓ 插入峰值 {i+1}，特徵 ID: {feature_id}")
        
        print()


def example_4_query_data():
    """範例 4: 查詢數據"""
    print("=" * 50)
    print("範例 4: 查詢數據")
    print("=" * 50)
    
    with DatabaseAPI("example.db") as db:
        # 查詢所有 DUT
        duts = db.get_duts()
        print(f"✓ 找到 {len(duts)} 個 DUT:")
        for dut in duts:
            print(f"  - DUT_id: {dut['DUT_id']}, Wafer: {dut['wafer']}, "
                  f"Die: {dut['die']}, Device: {dut['device']}")
        
        # 查詢特定 DUT 的所有會話
        if duts:
            dut_id = duts[0]['DUT_id']
            sessions = db.get_sessions_by_dut(dut_id)
            print(f"\n✓ DUT {dut_id} 有 {len(sessions)} 個測量會話:")
            for session in sessions:
                print(f"  - Session ID: {session['session_id']}, "
                      f"名稱: {session['session_name']}, "
                      f"操作員: {session['operator']}")
        
        # 查詢實驗條件
        if sessions:
            session_id = sessions[0]['session_id']
            conditions = db.get_conditions_dict(session_id)
            print(f"\n✓ 會話 {session_id} 的實驗條件:")
            for key, (value, unit) in conditions.items():
                unit_text = f" {unit}" if unit else ""
                print(f"  - {key}: {value}{unit_text}")

        # 查詢測量數據資訊
        if sessions:
            session_id = sessions[0]['session_id']
            data_list = db.get_measurement_data_by_session(session_id)
            if data_list:
                data_id = data_list[0]['data_id']
                info = db.get_data_info_dict(data_id)
                print(f"\n✓ 測量數據 {data_id} 的資訊:")
                for key, (value, unit) in info.items():
                    unit_text = f" {unit}" if unit else ""
                    print(f"  - {key}: {value}{unit_text}")
        
        print()


def example_5_query_full_session():
    """範例 5: 查詢完整的會話資訊"""
    print("=" * 50)
    print("範例 5: 查詢完整會話資訊")
    print("=" * 50)
    
    with DatabaseAPI("example.db") as db:
        # 獲取完整的會話資訊
        full_info = db.get_session_full_info(session_id=1)
        
        if full_info:
            print(f"✓ 會話名稱: {full_info['session_name']}")
            print(f"✓ 操作員: {full_info['operator']}")
            print(f"\n  DUT 資訊:")
            print(f"    - Wafer: {full_info['dut']['wafer']}")
            print(f"    - Die: {full_info['dut']['die']}")
            print(f"    - Device: {full_info['dut']['device']}")
            
            print(f"\n  實驗條件:")
            for key, (value, unit) in full_info['conditions'].items():
                unit_text = f" {unit}" if unit else ""
                print(f"    - {key}: {value}{unit_text}")
            
            print(f"\n  測量數據:")
            for data in full_info['measurement_data']:
                print(f"    - 類型: {data['data_type']}, 路徑: {data['file_path']}")
                info = db.get_data_info_dict(data['data_id'])
                if info:
                    print(f"      資訊:")
                    for key, (value, unit) in info.items():
                        unit_text = f" {unit}" if unit else ""
                        print(f"        {key}: {value}{unit_text}")
            
            print(f"\n  分析結果:")
            for analysis in full_info['analysis_runs']:
                print(f"    - 分析類型: {analysis['analysis_type']}")
                if analysis['inputs']:
                    print(f"      使用測量數據:")
                    for data in analysis['inputs']:
                        print(f"        - {data['data_type']}: {data['file_path']}")
                for feature in analysis['features']:
                    print(f"      峰值 {feature['feature_index']}:")
                    for key, (value, unit) in feature['values'].items():
                        print(f"        {key}: {value} {unit if unit else ''}")
        
        print()


def example_6_search_features():
    """範例 6: 搜索特定範圍的特徵"""
    print("=" * 50)
    print("範例 6: 搜索特徵")
    print("=" * 50)
    
    with DatabaseAPI("example.db") as db:
        # 搜索波長在 1550-1560 nm 範圍內的特徵
        features = db.search_features_by_value(key='wavelength',
                                               min_value=1550.0,
                                               max_value=1560.0,
                                               unit='nm')
        
        print(f"✓ 找到 {len(features)} 個符合條件的特徵:")
        for feature in features:
            print(f"  - 特徵 ID: {feature['feature_id']}, "
                  f"值: {feature['value']} {feature['unit']}")
        
        print()


def example_7_delete_data():
    """範例 7: 刪除數據"""
    print("=" * 50)
    print("範例 7: 刪除數據")
    print("=" * 50)
    
    with DatabaseAPI("example.db") as db:
        # 顯示刪除前的統計
        print("刪除前的統計:")
        stats = db.get_database_stats()
        for table, count in stats.items():
            print(f"  {table}: {count} 筆記錄")
        
        # 刪除一個會話（會級聯刪除相關的所有數據）
        # deleted = db.delete_session(session_id=1)
        # print(f"\n✓ 刪除了 {deleted} 個會話")
        
        # 刪除舊的會話（範例：刪除一週前的數據）
        # one_week_ago = datetime.now() - timedelta(days=7)
        # deleted = db.delete_old_sessions(one_week_ago)
        # print(f"✓ 刪除了 {deleted} 個舊會話")
        
        print("\n(註: 刪除操作已註解，取消註解可執行刪除)")
        print()


def example_8_batch_insert():
    """範例 8: 批量插入數據"""
    print("=" * 50)
    print("範例 8: 批量插入數據")
    print("=" * 50)
    
    with DatabaseAPI("example.db") as db:
        # 批量插入多個 DUT 和測量會話
        for i in range(3):
            # 使用 insert_dut（重複會自動忽略）
            dut_id = db.insert_dut(wafer=f"W{i+1:03d}",
                                   doe=f"DOE_{i+1}",
                                   die=i+1,
                                   cage=f"C{i+1}",
                                   device=f"D{i+1:03d}")
            
            # 為每個 DUT 創建多個測量會話
            for j in range(2):
                session_id = db.insert_measurement_session(dut_id=dut_id,
                                                           session_name=f"測量_{i+1}_{j+1}",
                                                           operator="李四",
                                                           system_version="v1.0")
                
                # 添加實驗條件（支援有單位格式）
                db.insert_experimental_conditions(session_id, {'temperature': (25.0 + i, '°C'),
                                                               'voltage': (3.3 + j * 0.1, 'V')})
                
                print(f"✓ 插入 DUT {i+1} 的會話 {j+1}")
        
        print(f"\n批量插入完成")
        stats = db.get_database_stats()
        print(f"現在有 {stats['DUT']} 個 DUT，{stats['MeasurementSessions']} 個會話")
        print()


def example_9_custom_query():
    """範例 9: 自定義查詢"""
    print("=" * 50)
    print("範例 9: 自定義查詢")
    print("=" * 50)
    
    with DatabaseAPI("example.db") as db:
        # 複雜的聯結查詢：找出所有有分析結果的會話
        sql = """
        SELECT DISTINCT
            ms.session_id,
            ms.session_name,
            d.wafer,
            d.die,
            COUNT(af.feature_id) as feature_count
        FROM MeasurementSessions ms
        JOIN DUT d ON ms.DUT_id = d.DUT_id
        LEFT JOIN AnalysisRuns ar ON ms.session_id = ar.session_id
        LEFT JOIN AnalysisFeatures af ON ar.analysis_id = af.analysis_id
        GROUP BY ms.session_id
        """
        
        results = db.query(sql)
        
        print(f"✓ 找到 {len(results)} 個會話:")
        for row in results:
            print(f"  - 會話: {row['session_name']}, "
                  f"Wafer: {row['wafer']}, "
                  f"特徵數: {row['feature_count']}")
        
        print()


def example_10_export_xlsx():
    """範例 10: 匯出所有資料表為 xlsx"""
    print("=" * 50)
    print("範例 10: 匯出 xlsx")
    print("=" * 50)

    with DatabaseAPI("example.db") as db:
        output_path = db.export_all_tables_to_xlsx("database_export.xlsx")
        print(f"✓ 已匯出: {output_path}")

    print()


def main():
    """執行所有範例"""
    print("\n" + "=" * 50)
    print("資料庫 API 使用範例")
    print("=" * 50 + "\n")
    
    # 創建資料庫
    example_1_create_database()
    
    # 插入數據
    example_2_insert_basic_data()
    example_3_insert_analysis_data()
    example_8_batch_insert()
    
    # 查詢數據
    example_4_query_data()
    example_5_query_full_session()
    example_6_search_features()
    example_9_custom_query()
    # 需要安裝 pandas/openpyxl 才能執行
    example_10_export_xlsx()
    
    # 刪除數據（已註解）
    example_7_delete_data()
    
    print("=" * 50)
    print("所有範例執行完成！")
    print("=" * 50)


if __name__ == "__main__":
    main()
