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
            return False, None, 0, []

        all_records = []
        table_name = None
        data_type = None

        for sheet_name, df in sheet_dict.items():
            # === 自动识别日期 ===
            match = re.search(r"\((\d{4}-\d{2}-\d{2})\)", sheet_name)
            data_date = datetime.datetime.strptime(match.group(1), "%Y-%m-%d").date()

            # === 根据文件名识别类型 ===
            file_name = str(excel_file)
            
            chinese_match = re.search(r'([\u4e00-\u9fff]+)', file_name)
            if chinese_match:
                data_type = chinese_match.group(1)
                print(f"📁 文件类型识别: {data_type}")
            else:
                print(f"⚠️ 未能在文件名中找到汉字：{file_name}，跳过。")
                return False, None, 0, []

            print(f"\n📘 正在处理 {sheet_name} | 日期: {data_date} | 类型: {data_type}")

            records = self.process_24h_data(df, data_date, sheet_name, data_type)
            all_records.extend(records)

        if not all_records:
            print("❌ 没有任何有效数据被导入")
            return False, None, 0, []

        # === 保存数据库 ===
        success, table_name, record_count, preview_data = self.save_to_database(all_records, data_date)
        return success, table_name, record_count, preview_data

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

    # def save_to_database(self, records, data_date):
    #     """按日期自动创建表并保存数据"""
    #     if not records:
    #         print("❌ 没有可保存的记录")
    #         return False, None, 0, []

    #     # 🧩 1. 如果传入的是 DataFrame，转成 list[dict]
    #     if isinstance(records, pd.DataFrame):
    #         records = records.to_dict# 获取前5行数据预览
    #     preview_stmt = text(f"SELECT * FROM {table_name} LIMIT 5")
    #     result = conn.execute(preview_stmt)
    #     # 修复：正确处理SQLAlchemy行对象
    #     preview_data = []
    #     for row in result:
    #         # 将行对象转换为字典
    #         preview_data.append(dict(zip(result.keys(), row)))(orient="records")
    def save_to_database(self, records, data_date):
        """按日期自动创建表并保存数据"""
        if not records:
            print("❌ 没有可保存的记录")
            return False, None, 0, []

        # 🧩 1. 如果传入的是 DataFrame，转成 list[dict]
        if isinstance(records, pd.DataFrame):
            records = records.to_dict(orient="records")

        if not isinstance(records, list):
            print(f"❌ records 类型错误: {type(records)}，应为 list[dict]")
            return False, None, 0, []

        # 🧩 2. 过滤无效记录
        valid_records = []
        for i, r in enumerate(records):
            if not isinstance(r, dict):
                continue
            required_fields = ["record_date", "record_time", "channel_name", "value", "type", "sheet_name"]
            if not all(k in r for k in required_fields):
                continue
            # 转 record_date
            if isinstance(r["record_date"], str):
                r["record_date"] = pd.to_datetime(r["record_date"]).date()
            valid_records.append(r)

        if not valid_records:
            print("❌ 没有可保存的有效记录")
            return False, None, 0, []

        # --- 生成按天表名 ---
        table_name = f"power_data_{data_date.strftime('%Y%m%d')}"
        preview_data = []

        try:
            with self.db_manager.engine.begin() as conn:
                # --- 创建表（如果不存在） ---
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    record_date DATE NOT NULL,
                    record_time TIME,
                    type VARCHAR(255),
                    channel_name VARCHAR(255),
                    value DECIMAL(10,2),
                    sheet_name VARCHAR(255)
                );
                """
                conn.execute(text(create_table_sql))
                print(f"✅ 表 {table_name} 已存在或创建成功")

                # --- 批量插入 ---
                insert_stmt = text(f"""
                INSERT INTO {table_name} 
                (record_date, record_time, type, channel_name, value, sheet_name)
                VALUES (:record_date, :record_time, :type, :channel_name, :value, :sheet_name)
                """)

                batch_size = 200
                for i in range(0, len(valid_records), batch_size):
                    batch = valid_records[i:i + batch_size]
                    conn.execute(insert_stmt, batch)
                    print(f"💾 已插入第 {i // batch_size + 1} 批数据 ({len(batch)} 条)")

                count_stmt = text(f"SELECT COUNT(*) FROM {table_name}")
                count = conn.execute(count_stmt).scalar()
                
                # 获取前5行数据预览
                preview_stmt = text(f"SELECT * FROM {table_name} ORDER BY id DESC LIMIT 5")
                result = conn.execute(preview_stmt)
                # 修复：正确处理SQLAlchemy行对象
                preview_data = []
                for row in result:
                    # 将行对象转换为字典
                    preview_data.append(dict(zip(result.keys(), row)))
                
                print(f"✅ 数据库保存成功: {count} 条记录")
                return True, table_name, count, preview_data

        except Exception as e:
            print(f"❌ 数据库保存失败: {e}")
            import traceback
            traceback.print_exc()
            return False, None, 0, []

    def import_custom_excel(self, excel_file):
        """导入指定的5个sheet，并按固定规则映射"""
        try:
            # 读取所有sheet
            sheet_dict = pd.read_excel(excel_file, sheet_name=None, header=None)
        except Exception as e:
            print(f"❌ 无法读取Excel: {e}")
            return False
        file_name = str(excel_file)
        
        chinese_match = re.search(r'([\u4e00-\u9fff]+)', file_name)
        if chinese_match:
            data_type = chinese_match.group(1) + "实际信息"
            print(f"📁 文件类型识别: {data_type}")
        else:
            print(f"⚠️ 未能在文件名中找到汉字：{file_name}，跳过。")
            return False
        sheet_names = list(sheet_dict.keys())
        print(f"📘 检测到 {len(sheet_names)} 个Sheet: {sheet_names}")

        # 要处理的sheet编号（1-based）
        target_indexes = [0, 1, 3, 4, 5,6,-2,-1]  # 对应第1,2,4,5,6个sheet

        all_records = []

        for i in target_indexes:
            if i >= len(sheet_names):
                print(f"⚠️ Excel中不存在第{i+1}个sheet，跳过")
                continue

            sheet_name = sheet_names[i]
            df = sheet_dict[sheet_name]
            print(f"\n🔹 正在处理 Sheet {i+1}: {sheet_name}")

            # 统一识别日期
            match = re.search(r"\((\d{4}-\d{2}-\d{2})\)", sheet_name)
            data_date = datetime.datetime.strptime(match.group(1), "%Y-%m-%d").date()
            # 根据sheet序号调用不同映射函数
            if i in [0, 3, 4]:  # 第1,4,5个sheet：时刻→channel_name
                records = self._process_time_as_channel(df, data_date, sheet_name, data_type)
            elif i in [1, 5]:  # 第2,6个sheet：第一行→channel_name
                records = self._process_first_row_as_channel(df, data_date, sheet_name, data_type)
            elif i in [6]:
                records = self._process_fsc_as_channel(df, data_date, sheet_name, data_type)
            # elif i in [-2]:
            #     records = self._process_new_as_table(df, data_date, sheet_name, data_type)
            else:
                print(f"⚠️ 第{i+1}个sheet未定义处理规则，跳过")
                continue

            print(f"✅ Sheet{i+1} 处理完成，共 {len(records)} 条记录")
            all_records.extend(records)

        if not all_records:
            print("❌ 没有生成任何有效记录")
            return False

        return self.save_to_database(all_records, data_date)

    def _process_time_as_channel(self, df, data_date, sheet_name, data_type):
        """将时刻列名映射为channel_name"""
        records = []
        df = df.dropna(how="all")  # 删除空行

        # 如果第一列是 “时刻” 字样
        if str(df.iloc[0, 0]).strip() == "时刻":
            df.columns = [str(c).strip() for c in df.iloc[0]]  # 第一行作列名
            df = df[1:]  # 去掉标题行
        else:
            df.columns = [str(c).strip() for c in df.columns]

        # 查找时间列（形如 00:00、01:15）
        time_cols = [c for c in df.columns if re.match(r"\d{2}:\d{2}", c)]
        if not time_cols:
            print(f"⚠️ 未找到时间列: {df.columns.tolist()}")
            return []
        # 遍历每一行（每一类指标）
        for _, row in df.iterrows():
            # 跳过无效行或标题行
            if not isinstance(row[time_cols[0]], (int, float)) and not str(row[time_cols[0]]).replace('.', '', 1).isdigit():
                continue

            # 指标名（比如 “统调负荷(MW)”）
            indicator_name = str(row.get("时刻") or row.index[0]).strip()

            for t in time_cols:
                value = row[t]
                if pd.isna(value):
                    continue
                try:
                    value = float(value)
                except:
                    continue  # 跳过非数值的单元格
                record = {
                    "record_date": data_date,
                    "record_time": t,
                    "channel_name": indicator_name,  # 用指标名作通道名
                    "value": value,
                    "type": data_type,
                    "sheet_name": sheet_name,
                    "created_at": datetime.datetime.now(),
                }
                records.append(record)
        return records

    def _process_fsc_as_channel(self, df, data_date, sheet_name, data_type):
        """将时刻列名映射为channel_name"""
        records = []
        df = df.dropna(how="all")  # 删除空行
        df.columns = [str(c).strip() for c in df.iloc[0]]  # 第一行作列名
        df = df[1:]  # 去掉标题行
        
        first_col = df.columns[0]
        second_col = df.columns[1]
       
        # 查找时间列（形如 00:00、01:15 或数字格式 0, 1, 2...）
        time_cols = [c for c in df.columns if re.match(r"\d{2}:\d{2}", c)]
        if not time_cols:
            print(f"⚠️ 未找到时间列: {df.columns.tolist()}")
            return []

        # 遍历每一行（每一类指标）
        for _, row in df.iterrows():
            # 跳过无效行或标题行
            if not isinstance(row[time_cols[0]], (int, float)) and not str(row[time_cols[0]]).replace('.', '', 1).isdigit():
                continue
            
            # 生成 channel_name：第一列和第二列用下划线连接
            channel_name = f"{row[first_col]}_{row[second_col]}"

            for t in time_cols:
                value = row[t]
                if pd.isna(value):
                    continue
                try:
                    value = float(value)
                except:
                    continue  # 跳过非数值的单元格

                record = {
                    "record_date": data_date,
                    "record_time": t,
                    "channel_name": channel_name,  # 用指标名作通道名
                    "value": value,
                    "type": data_type,
                    "sheet_name": sheet_name,
                    "created_at": datetime.datetime.now(),
                }
                records.append(record)
        return records
    def _process_first_row_as_channel(self, df, data_date, sheet_name, data_type):
        """
        处理格式：
        最高负荷(MW) | 最低负荷(MW) | 平均负荷(MW)
        243330.375    | 182924.0156  | 212967.9509
        """
        records = []
        # 删除空行与空列
        df = df.dropna(how="all").dropna(axis=1, how="all")
        if df.empty:
            print(f"⚠️ Sheet {sheet_name} 为空，跳过。")
            return records

        # 第一行作为 channel_name
        channel_names = [str(c).strip() for c in df.iloc[0].tolist()]
        df = df.iloc[1:]  # 从第二行开始为数据
        if df.empty:
            print(f"⚠️ Sheet {sheet_name} 仅有表头，无数据。")
            return records

        for _, row in df.iterrows():
            for col_idx, value in enumerate(row):
                if col_idx >= len(channel_names):
                    continue
                if pd.isna(value):
                    continue
                record = {
                    "record_date": data_date,
                    "record_time": None,  # 没有时间列
                    "channel_name": channel_names[col_idx],
                    "value": value,
                    "type": data_type,
                    "sheet_name": sheet_name,
                    "created_at": datetime.datetime.now(),
                }
                records.append(record)

        print(f"✅ Sheet {sheet_name} 解析完成，共 {len(records)} 条记录。")
        return records

    def import_custom_excel_pred(self, excel_file):
            """导入指定的5个sheet，并按固定规则映射"""
            try:
                # 读取所有sheet
                sheet_dict = pd.read_excel(excel_file, sheet_name=None, header=0)
            except Exception as e:
                print(f"❌ 无法读取Excel: {e}")
                return False
            file_name = str(excel_file)
            
            chinese_match = re.search(r'([\u4e00-\u9fff]+)', file_name)
            if chinese_match:
                data_type = chinese_match.group(1) + "预测信息"
                print(f"📁 文件类型识别: {data_type}")
            else:
                print(f"⚠️ 未能在文件名中找到汉字：{file_name}，跳过。")
                return False
            sheet_names = list(sheet_dict.keys())
            print(f"📘 检测到 {len(sheet_names)} 个Sheet: {sheet_names}")

            # 要处理的sheet编号（1-based）
            target_indexes = [0, 1, 2, -3, -2, -1]  # 对应第1,2,4,5,6个sheet

            all_records = []

            for i in target_indexes:
                if i >= len(sheet_names):
                    print(f"⚠️ Excel中不存在第{i+1}个sheet，跳过")
                    continue

                sheet_name = sheet_names[i]
                df = sheet_dict[sheet_name]
                print(f"\n🔹 正在处理 Sheet {i+1}: {sheet_name}")

                # 统一识别日期
                match = re.search(r"\((\d{4}-\d{2}-\d{2})\)", sheet_name)
                data_date = datetime.datetime.strptime(match.group(1), "%Y-%m-%d").date()

                # 根据sheet序号调用不同映射函数
                if i in [0]:  # 第1个sheet：时刻→channel_name
                    records = self._process_time_as_channel(df, data_date, sheet_name, data_type)
                elif i in [1]: 
                    records = self._process_1_channel(df, data_date, sheet_name, data_type)
                elif i in [2]:  # 第3个sheet：时刻→channel_name
                    records = self._process_type_date_value(df, data_date, sheet_name, data_type)
                elif i in [-3]:  # 第4,5个sheet：时刻→channel_name
                    records = self._process_3_channel(df, data_date, sheet_name, data_type)
                elif i in [-2, -1]:  # 第7,8个sheet：第一行→channel_name
                    records = self._process_2_channel(df, data_date, sheet_name, data_type)
                else:
                    print(f"⚠️ 第{i+1}个sheet未定义处理规则，跳过")
                    continue

                print(f"✅ Sheet{i+1} 处理完成，共 {len(records)} 条记录")
                all_records.extend(records)

            if not all_records:
                print("❌ 没有生成任何有效记录")
                return False

            return self.save_to_database(all_records, data_date)
        
    def _process_1_channel(self, df, data_date, sheet_name, data_type):
        """
        多指标时刻型sheet处理：
        - 识别“类型 + 电源类型”为 channel_name
        - 使用“日期”列作为 record_date
        - 时间列为 00:00、00:15 等常规格式
        """
        import datetime
        import pandas as pd
        import re

        records = []
        df = df.dropna(how="all")
        df.columns = [str(c).strip() for c in df.columns]

        # 1️⃣ 找时间列（如 00:00、01:15 等）
        time_cols = [c for c in df.columns if re.match(r"^\d{1,2}:\d{2}$", c)]
        if not time_cols:
            print(f"⚠️ 未找到时间列: {df.columns.tolist()}")
            return []

        # 2️⃣ 识别辅助列
        col_type = "类型" if "类型" in df.columns else None
        col_date = "日期" if "日期" in df.columns else None
        col_power = "电源类型" if "电源类型" in df.columns else None

        # 3️⃣ 遍历每一行（每个通道）
        for _, row in df.iterrows():
            # --- 日期列 ---
            record_date = data_date
            if col_date and pd.notna(row[col_date]):
                try:
                    # 自动识别日期格式
                    record_date = pd.to_datetime(str(row[col_date]), errors="coerce").date()
                except:
                    record_date = data_date

            # --- 通道名：类型 + 电源类型 ---
            parts = []
            if col_type and pd.notna(row[col_type]):
                parts.append(str(row[col_type]).strip())
            if col_power and pd.notna(row[col_power]):
                parts.append(str(row[col_power]).strip())
            if not parts:
                continue
            channel_name = "-".join(parts)

            # --- 遍历时间列 ---
            for t in time_cols:
                value = row[t]
                if pd.isna(value):
                    continue
                try:
                    value = float(value)
                except:
                    continue

                records.append({
                    "record_date": record_date,        # 确保是 date 类型
                    "record_time": t,                  # 如 00:00
                    "channel_name": channel_name,      # 如 "现货新能源总出力(MW)-风电"
                    "value": value,
                    "type": data_type,
                    "sheet_name": sheet_name,
                    "created_at": datetime.datetime.now(),
                })

        print(f"✅ {sheet_name} 解析完成，共 {len(records)} 条记录")
        return records

    def _process_3_channel(self, df, data_date, sheet_name, data_type):
        """
        将多列通道型Sheet处理成记录列表，每列视为一个通道。
        结构示例：
        序号 | 日期 | 必开机组容量(MW) | 必停机组容量(MW)
        """
        import datetime
        import pandas as pd

        print(f"🔹 正在处理 Sheet: {sheet_name}")

        records = []

        # 1️⃣ 删除无用列
        if "序号" in df.columns:
            df = df.drop(columns=["序号"])

        # 2️⃣ 确保日期字段存在
        if "日期" not in df.columns:
            print(f"⚠️ 未找到日期列，跳过 {sheet_name}")
            return []

        df = df.dropna(how="all")
        df.columns = [str(c).strip() for c in df.columns]

        # 3️⃣ 遍历每一行
        for _, row in df.iterrows():
            # 日期
            record_date = data_date
            if pd.notna(row["日期"]):
                try:
                    record_date = pd.to_datetime(str(row["日期"]), errors="coerce").date()
                except:
                    record_date = data_date

            # 4️⃣ 遍历通道列（除“日期”外）
            for col in df.columns:
                if col in ["日期"]:
                    continue
                value = row[col]
                if pd.isna(value):
                    continue
                try:
                    value = float(value)
                except:
                    continue

                records.append({
                    "record_date": record_date,
                    "record_time": None,
                    "channel_name": col,
                    "value": value,
                    "type": data_type,
                    "sheet_name": sheet_name,
                    "created_at": datetime.datetime.now(),
                })

        print(f"✅ {sheet_name} 处理完成，共 {len(records)} 条记录")
        return records
    
    def _process_2_channel(self, df, data_date, sheet_name, data_type):
        """
        处理机组名单表：
        - channel_name = 电厂名称-机组名称-类型
        - value 默认为 1
        """
        import datetime
        import pandas as pd

        records = []

        if df.empty:
            print(f"⚠️ {sheet_name} 表为空，跳过")
            return []

        df = df.dropna(how="all")
        df.columns = [str(c).strip() for c in df.columns]

        # 必要列
        col_date = "日期" if "日期" in df.columns else None
        col_plant = "电厂名称" if "电厂名称" in df.columns else None
        col_unit = "机组名称" if "机组名称" in df.columns else None
        col_type = "类型" if "类型" in df.columns else None

        for _, row in df.iterrows():
            # 日期
            record_date = data_date
            if col_date and pd.notna(row[col_date]):
                try:
                    record_date = pd.to_datetime(str(row[col_date]), errors="coerce").date()
                except:
                    record_date = data_date

            # channel_name 拼接
            parts = []
            for col in [col_plant, col_unit, col_type]:
                if col and pd.notna(row[col]):
                    parts.append(str(row[col]).strip())
            if not parts:
                continue
            channel_name = "-".join(parts)

            # 添加记录
            records.append({
                "record_date": record_date,
                "channel_name": channel_name,
                "record_time": None,
                "value": None,
                "type": data_type,
                "sheet_name": sheet_name,
                "created_at": datetime.datetime.now(),
            })

        print(f"✅ {sheet_name} 处理完成，共 {len(records)} 条记录")
        return records

    def _process_type_date_value(self, df, data_date, sheet_name, data_type):
        """处理类似 '类型 日期 数值' 的结构（无时间列，record_date为date类型）"""
        records = []
        df = df.dropna(how="all")
        df.columns = [str(c).strip() for c in df.columns]

        # 查找列
        col_type = "类型" if "类型" in df.columns else None
        col_date = "日期" if "日期" in df.columns else None

        # 查找数值列（排除掉已知列）
        value_cols = [c for c in df.columns if c not in [col_type, col_date]]
        if not value_cols:
            print(f"⚠️ 未找到数值列: {df.columns.tolist()}")
            return []

        value_col = value_cols[0]  # 默认只取第一列数值

        for _, row in df.iterrows():
            channel_name = str(row[col_type]).strip() if col_type else "未知类型"
            raw_date = str(row[col_date]).strip() if col_date and pd.notna(row[col_date]) else None

            # === 日期解析逻辑 ===
            parsed_date = None
            if raw_date:
                # 1. 如果是标准日期格式
                try:
                    parsed_date = pd.to_datetime(raw_date).date()
                except Exception:
                    pass

                # 2. 如果是形如 “2025年第38周(09.15~09.21)”
                if parsed_date is None:
                    match = re.search(r"\((\d{2})\.(\d{2})", raw_date)
                    year_match = re.search(r"(\d{4})年", raw_date)
                    if match and year_match:
                        year = int(year_match.group(1))
                        month = int(match.group(1))
                        day = int(match.group(2))
                        parsed_date = datetime.date(year, month, day)

            # 如果都解析失败，则用 data_date 兜底
            if parsed_date is None:
                parsed_date = pd.to_datetime(data_date).date()

            # 数值
            try:
                value = float(row[value_col])
            except:
                continue

            record = {
                "record_date": parsed_date,
                "record_time": datetime.datetime.now().time(),
                "channel_name": channel_name,
                "value": value,
                "type": data_type,
                "sheet_name": sheet_name,
                "created_at": datetime.datetime.now(),
            }
            records.append(record)

        return records
    
    def import_point_data(self, excel_file):
        """自动导入Excel第一个Sheet的数据，并按列求均值"""
        import re
        import datetime
        import pandas as pd

        try:
            xls = pd.ExcelFile(excel_file)
            first_sheet_name = xls.sheet_names[0]  # ✅ 获取第一个 sheet 名
            df = pd.read_excel(excel_file, sheet_name=first_sheet_name, header=0)
            print(f"✅ 成功读取 Excel: {excel_file}, sheet: {first_sheet_name}")
        except Exception as e:
            print(f"❌ 读取 Excel 失败: {e}")
            return False, None, 0, []

        # 自动识别日期
        match = re.search(r"\((\d{4}-\d{2}-\d{2})\)", first_sheet_name)
        data_date = datetime.datetime.strptime(match.group(1), "%Y-%m-%d").date()

        # 根据文件名识别类型
        file_name = str(excel_file)
        chinese_match = re.search(r'([\u4e00-\u9fff]+)', file_name)
        if chinese_match:
            data_type = chinese_match.group(1)
            print(f"📁 文件类型识别: {data_type}")
        else:
            print(f"⚠️ 未能在文件名中找到汉字：{file_name}，跳过。")
            return False, None, 0, []

        print(f"\n📘 正在处理 {first_sheet_name} | 日期: {data_date} | 类型: {data_type}")

        # 按列求均值并生成 records
        records = self.process_mean_by_column(df, data_date, first_sheet_name, data_type)

        if not records:
            print("❌ 没有任何有效数据被导入")
            return False, None, 0, []

        # 保存到数据库
        success, table_name, record_count, preview_data = self.save_to_database(records, data_date)
        print(f"✅ 数据保存成功，表名: {table_name}，记录数: {record_count}")
        return success, table_name, record_count, preview_data

    def process_mean_by_column(self, df, data_date, sheet_name, data_type):
        """
        针对节点电价等表格：对每一列（从第3列开始）求均值，并生成记录
        毎一列の均値データ放在最後，其他データ按順序都存一下
        """
        records = []

        # 标准化列名
        df.columns = [str(c).strip() for c in df.columns]
        # print(f"COLUMNS: {df.columns.tolist()}")

        # 获取时间列（第3列及之后）
        time_cols = df.columns[2:]
        if time_cols.empty or len(time_cols) == 0:
            print(f"⚠️ Sheet {sheet_name} 没有发现时间列")
            return records

        # 将时间列按每4个分组（每小时4个15分钟间隔）
        time_groups = {}
        for t in time_cols:
            # 从 "HH:MM" 格式中提取小时
            hour = t.split(':')[0]
            if hour not in time_groups:
                time_groups[hour] = []
            time_groups[hour].append(t)

        # 先保存原有的数据（按小时分组）
        # 预先计算每行每小时的均值
        hourly_means = {}  # {(row_index, hour): mean_value}
        
        for _, row in df.iterrows():
            # 检查第一列是否有有效数据，如果没有则跳过（处理标题行）
            channel_name = row.iloc[0]  # 第一列作为通道名称
            if pd.isna(channel_name) or channel_name == "":
                continue
                
            # 为每行每小时计算均值
            for hour, times in time_groups.items():
                # 计算该小时内四个时间点的均值
                values = []
                for t in times:
                    value = row[t]
                    if not pd.isna(value):
                        values.append(value)
                
                # 如果有有效值，则计算均值
                if values:
                    hourly_mean = sum(values) / len(values)
                    hourly_means[(_, hour)] = hourly_mean
                    
                    record = {
                        "record_date": pd.to_datetime(data_date).date(),
                        "record_time": f"{hour}:00",  # 按小时存储
                        "channel_name": channel_name,
                        "value": round(hourly_mean, 2),  # 使用该小时内四个时间点的均値
                        "type": data_type,
                        "sheet_name": sheet_name,
                        "created_at": pd.Timestamp.now(),
                    }
                    records.append(record)

        # 再添加每小时的均値データ（所有行在该小时的均値）
        for hour, times in time_groups.items():
            # 获取这些时间点的値并計算均値
            values = []
            for t in times:
                # 計算該時間点在所有行中的均値
                mean_value = df[t].mean()
                values.append(mean_value)
            
            # 計算4つの時間点の総均値
            if values:
                overall_mean = sum(values) / len(values)
                record = {
                    "record_date": pd.to_datetime(data_date).date(),
                    "record_time": f"{hour}:00",   # "HH:00" にフォーマット
                    "channel_name": f"{data_type}_均值",
                    "value": round(overall_mean, 2),
                    "type": data_type,
                    "sheet_name": sheet_name,
                    "created_at": pd.Timestamp.now(),
                }
                records.append(record)

        print(f"✅ {data_type} 均値生成 {len(records)} 条記錄")
        return records

    def query_daily_averages(self, date_list, data_type_keyword="日前节点电价"):
        """
        查询多天的均值数据（适用于已计算好的均值记录）
        
        Args:
            date_list (list): 日期列表，格式为 "YYYY-MM-DD"
            data_type_keyword (str): 数据类型关键字，用于筛选特定类型的数据
            
        Returns:
            dict: 包含查询结果的字典
        """
        try:
            # 构造表名列表
            table_names = []
            for date_str in date_list:
                # 将日期格式转换为表名格式 (YYYY-MM-DD -> YYYYMMDD)
                date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d")
                table_name = f"power_data_{date_obj.strftime('%Y%m%d')}"
                print(f"🔍 查询表: {table_name}")
                table_names.append(table_name)
                
            
            # 验证表是否存在
            existing_tables = self.db_manager.get_tables()
            valid_tables = [table for table in table_names if table in existing_tables]
            
            if not valid_tables:
                return {"data": [], "total": 0, "message": "没有找到有效的数据表"}
            
            # 构造UNION查询语句：查找包含指定关键字和"均值"的记录
            union_parts = []
            for table in valid_tables:
                union_parts.append(f""" SELECT * FROM {table} WHERE channel_name LIKE '%均值%' AND channel_name LIKE '%{data_type_keyword}%'""")
            
            if not union_parts:
                return {"data": [], "total": 0, "message": "没有找到匹配的数据"}
                
            union_query = " UNION ALL ".join(union_parts)
            print(f"🚀 执行UNION查询: {union_query}")
            final_query = f"""
                SELECT * FROM ({union_query}) as combined_data
                ORDER BY record_date, record_time
            """
            
            # 执行查询
            result = self.db_manager.complex_query(final_query)
            # print(f"✅ 查询成功，共 {len(result)} 条记录")
            # print(result)
            
            # 构造返回结果
            return {
                "data": result.get("data"),
                "total": result.get("total"),
                "message": "查询成功"
            }
            
        except Exception as e:
            print(f"❌ 查询多天均值数据失败: {str(e)}")
            import traceback
            traceback.print_exc()
            return {"data": [], "total": 0, "message": f"查询失败: {str(e)}"}



    def save_to_new_table(self, records, table_name):
        """
        保存记录到新表
        
        Args:
            records: 记录列表
            table_name: 表名
            
        Returns:
            tuple: (success: bool, table_name: str, record_count: int, preview_data: list)
        """
        if not records:
            print("❌ 没有可保存的记录")
            return False, None, 0, []

        # 如果传入的是 DataFrame，转成 list[dict]
        if isinstance(records, pd.DataFrame):
            records = records.to_dict(orient="records")

        if not isinstance(records, list):
            print(f"❌ records 类型错误: {type(records)}，应为 list[dict]")
            return False, None, 0, []

        # 过滤无效记录
        valid_records = []
        for i, r in enumerate(records):
            if not isinstance(r, dict):
                continue
            # 不强制要求特定字段，因为我们是创建新表
            valid_records.append(r)

        if not valid_records:
            print("❌ 没有可保存的有效记录")
            return False, None, 0, []

        preview_data = []

        try:
            with self.db_manager.engine.begin() as conn:
                # 创建新表
                create_table_sql = f"""
                CREATE TABLE {table_name} (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    record_date DATE,
                    record_time VARCHAR(10),
                    type VARCHAR(255),
                    channel_name VARCHAR(255),
                    value DECIMAL(15,4),
                    sheet_name VARCHAR(255),
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
                try:
                    conn.execute(text(create_table_sql))
                    print(f"✅ 表 {table_name} 创建成功")
                except Exception as e:
                    if "already exists" in str(e) or "already exist" in str(e):
                        print(f"⚠️ 表 {table_name} 已存在")
                    else:
                        raise e

                # 准备插入语句（只插入存在的字段）
                if valid_records:
                    # 获取第一条记录的字段
                    sample_record = valid_records[0]
                    fields = [k for k in sample_record.keys() if k in [
                        "record_date", "record_time", "type", "channel_name", "value", "sheet_name"]]
                    
                    field_placeholders = ", ".join(fields)
                    value_placeholders = ", ".join([f":{field}" for field in fields])
                    
                    insert_stmt = text(f"""
                    INSERT INTO {table_name} ({field_placeholders})
                    VALUES ({value_placeholders})
                    """)

                    # 批量插入
                    batch_size = 200
                    for i in range(0, len(valid_records), batch_size):
                        batch = valid_records[i:i + batch_size]
                        # 只保留存在的字段
                        filtered_batch = []
                        for record in batch:
                            filtered_record = {k: v for k, v in record.items() if k in fields}
                            filtered_batch.append(filtered_record)
                        
                        conn.execute(insert_stmt, filtered_batch)
                        print(f"💾 已插入第 {i // batch_size + 1} 批数据 ({len(filtered_batch)} 条)")

                # 获取记录总数
                count_stmt = text(f"SELECT COUNT(*) FROM {table_name}")
                count = conn.execute(count_stmt).scalar()
                
                # 获取前5行数据预览
                preview_stmt = text(f"SELECT * FROM {table_name} ORDER BY id DESC LIMIT 5")
                result = conn.execute(preview_stmt)
                # 修复：正确处理SQLAlchemy行对象
                preview_data = []
                for row in result:
                    # 将行对象转换为字典
                    preview_data.append(dict(zip(result.keys(), row)))
                
                print(f"✅ 数据库保存成功: {count} 条记录")
                return True, table_name, count, preview_data

        except Exception as e:
            print(f"❌ 数据库保存失败: {e}")
            import traceback
            traceback.print_exc()
            return False, None, 0, []
