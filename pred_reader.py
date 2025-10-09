import pandas as pd
import datetime
import re
from sqlalchemy import text
from database import DatabaseManager

class PowerDataImporter:
    def __init__(self):
        self.db_manager = DatabaseManager()
        pass

    # ===============================
    # 主入口：导入所有sheet
    # ===============================
    def import_power_data(self, excel_file):
        """自动导入Excel中所有Sheet的数据，日期自动识别"""
        sheet_dict = self.read_excel_data(excel_file)
        if not sheet_dict:
            return False

        all_records = []

        for sheet_name, df in sheet_dict.items():
            # === 自动识别日期 ===
            match = re.search(r"\((\d{4}-\d{2}-\d{2})\)", sheet_name)
            data_date = match.group(1) if match else datetime.datetime.now().strftime('%Y-%m-%d')

            # === 根据文件名识别类型 ===
            file_name = str(excel_file)
            
            chinese_match = re.search(r'([\u4e00-\u9fff]+)', file_name)
            if chinese_match:
                data_type = chinese_match.group(1)
                print(f"📁 文件类型识别: {data_type}")
            else:
                print(f"⚠️ 未能在文件名中找到汉字：{file_name}，跳过。")
                return False

            print(f"\n📘 正在处理 {sheet_name} | 日期: {data_date} | 类型: {data_type}")

            records = self.process_24h_data(df, data_date, sheet_name, data_type)
            all_records.extend(records)

        if not all_records:
            print("❌ 没有任何有效数据被导入")
            return False

        # === 保存数据库 ===
        return self.save_to_database(all_records, data_date)

    # ===============================
    # 读取所有sheet
    # ===============================
    def read_excel_data(self, excel_file):
        """读取Excel中所有Sheet"""
        try:
            sheet_dict = pd.read_excel(excel_file, sheet_name=None, header=0)
            print(f"✅ 成功读取Excel，共 {len(sheet_dict)} 个Sheet: {list(sheet_dict.keys())}")
            return sheet_dict
        except Exception as e:
            print(f"❌ 读取Excel失败: {e}")
            return None

    # ===============================
    # 处理单个sheet的24小时数据
    # ===============================
    def process_24h_data(self, df, data_date, sheet_name, data_type):
        """处理单个Sheet（行式结构）的24小时数据"""
        records = []

        # 标准化列名
        df.columns = [str(c).strip() for c in df.columns]

        # 检查数据格式：有"通道名称"列还是有"类型"列
        if "通道名称" in df.columns:
            records = self._process_channel_format(df, data_date, sheet_name, data_type)
        elif "类型" in df.columns:
            records = self._process_type_format(df, data_date, sheet_name, data_type)
        else:
            print(f"⚠️ 未找到 '通道名称' 或 '类型' 列，跳过。可用列: {list(df.columns)}")
            return records

        print(f"✅ {data_type} 导入 {len(records)} 条记录")
        return records

    def _process_channel_format(self, df, data_date, sheet_name, data_type):
        """处理有'通道名称'列的数据格式"""
        records = []

        # 直接使用所有有通道名称的行
        valid_rows = df[df["通道名称"].notna()]
        if valid_rows.empty:
            print(f"⚠️ Sheet中无有效通道，通道列值为: {df['通道名称'].unique().tolist()}")
            return records

        # 提取所有时间列（一般从00:00到23:45）
        time_cols = [c for c in df.columns if re.match(r"\d{2}:\d{2}", c)]
        if not time_cols:
            print(f"⚠️ 没有发现时间列: {list(df.columns)}")
            return records

        # 遍历每一行（一个通道）
        for _, row in valid_rows.iterrows():
            channel_name = row["通道名称"]

            for t in time_cols:
                # 处理NaN值，跳过NULL值
                value = row[t]
                if pd.isna(value):
                    continue  # 跳过这个记录
                
                record = {
                    "record_date": data_date,
                    "record_time": t,
                    "channel_name": channel_name,
                    "value": value,
                    "type": data_type,
                    "sheet_name": sheet_name,
                    "created_at": datetime.datetime.now(),
                }
                records.append(record)

        return records

    def _process_type_format(self, df, data_date, sheet_name, data_type):
        """处理有'类型'列的数据格式"""
        records = []

        # 直接使用所有有类型名称的行
        valid_rows = df[df["类型"].notna()]
        if valid_rows.empty:
            print(f"⚠️ Sheet中无有效类型，类型列值为: {df['类型'].unique().tolist()}")
            return records

        # 提取所有时间列（一般从00:00到23:45）
        time_cols = [c for c in df.columns if re.match(r"\d{2}:\d{2}", c)]
        if not time_cols:
            print(f"⚠️ 没有发现时间列: {list(df.columns)}")
            return records

        # 遍历每一行（一个类型）
        for _, row in valid_rows.iterrows():
            channel_name = row["类型"]  # 将"类型"列的值作为channel_name

            for t in time_cols:
                # 处理NaN值，跳过NULL值
                value = row[t]
                if pd.isna(value):
                    continue  # 跳过这个记录
                
                record = {
                    "record_date": data_date,
                    "record_time": t,
                    "channel_name": channel_name,
                    "value": value,
                    "type": data_type,
                    "sheet_name": sheet_name,
                    "created_at": datetime.datetime.now(),
                }
                records.append(record)

        return records

    # -------------------------------
    # 数据保存
    # -------------------------------
    def save_to_database(self, records, data_date):
        """保存所有Sheet的数据到数据库"""
        if not records:
            print("❌ 没有可保存的记录")
            return False

        try:
            with self.db_manager.engine.begin() as conn:
                # 删除该日期数据
                delete_stmt = text("""
                    DELETE FROM power_data 
                    WHERE record_date = :record_date
                """)
                conn.execute(delete_stmt, {"record_date": data_date})
                print(f"🗑️ 已删除 {data_date} 的旧数据")

                # 插入新数据
                insert_stmt = text("""
                    INSERT INTO power_data 
                    (record_date, record_time, type, channel_name, value, sheet_name)
                    VALUES (:record_date, :record_time, :type, :channel_name, :value, :sheet_name)
                """)

                batch_size = 200
                for i in range(0, len(records), batch_size):
                    batch = records[i:i + batch_size]
                    conn.execute(insert_stmt, batch)
                    print(f"💾 已插入第 {i // batch_size + 1} 批数据 ({len(batch)} 条)")

                count_stmt = text("""
                    SELECT COUNT(*) FROM power_data WHERE record_date = :record_date
                """)
                count = conn.execute(count_stmt, {"record_date": data_date}).scalar()
                print(f"✅ 数据库保存成功: {count} 条记录")
                return True
        except Exception as e:
            print(f"❌ 数据库保存失败: {e}")
            import traceback
            traceback.print_exc()
            return False