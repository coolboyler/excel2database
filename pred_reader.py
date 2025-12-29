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
    # 保存数据到数据库
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

                count_stmt = text(f"SELECT COUNT(*) FROM {table_name} WHERE record_date = :record_date")
                count = conn.execute(count_stmt, {"record_date": data_date}).scalar()
                
                # 获取前5行数据预览
                preview_stmt = text(f"SELECT * FROM {table_name} WHERE record_date = :record_date ORDER BY id DESC LIMIT 5")
                result = conn.execute(preview_stmt, {"record_date": data_date})
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
    def save_to_outage_database(self, records, data_date):
        """保存停电数据到固定表 power_outage"""
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
            required_fields = ["device_name", "voltage_level", "device_type", "device_code", 
                        "planned_power_off_time", "actual_power_off_time", "planned_power_on_time","actual_power_on_time"]
            if not all(k in r for k in required_fields):
                continue
            # 添加 record_date 字段
            r["record_date"] = data_date
            valid_records.append(r)

        if not valid_records:
            print("❌ 没有可保存的有效记录")
            return False, None, 0, []

        # --- 使用固定表名 ---
        table_name = "power_outage"
        preview_data = []

        try:
            with self.db_manager.engine.begin() as conn:
                # --- 创建表（如果不存在） ---
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS `{table_name}` (
                    `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
                    `record_date` date NOT NULL COMMENT '记录日期',
                    `device_name` varchar(200) NOT NULL COMMENT '设备名称（如101变压器开关、220kV#1主变）',
                    `voltage_level` varchar(50) DEFAULT NULL COMMENT '电压等级（允许为空，部分设备可能未记录）',
                    `device_type` varchar(100) NOT NULL COMMENT '设备类型（如开关、主变、母线）',
                    `device_code` varchar(50) NOT NULL COMMENT '设备编号（唯一标识）',
                    `planned_power_off_time` datetime DEFAULT NULL COMMENT '计划停电日期时间（格式：YYYY-MM-DD HH:MM:SS）',
                    `actual_power_off_time` datetime DEFAULT NULL COMMENT '实际停电日期时间（格式：YYYY-MM-DD HH:MM:SS）',
                    `planned_power_on_time` datetime DEFAULT NULL COMMENT '计划复电日期时间（格式：YYYY-MM-DD HH:MM:SS）',
                    `actual_power_on_time` datetime DEFAULT NULL COMMENT '实际复电日期时间（格式：YYYY-MM-DD HH:MM:SS）',
                    `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
                    `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
                    `sheet_name` varchar(255) DEFAULT NULL COMMENT '数据来源表名',
                    PRIMARY KEY (`id`),
                    UNIQUE KEY `uk_device_code` (`device_code`) COMMENT '设备编号唯一约束'
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='设备停电记录信息表';
                """
                conn.execute(text(create_table_sql))
                print(f"✅ 表 {table_name} 已存在或创建成功")

                # --- 批量插入 ---
                insert_stmt = text(f"""
                INSERT IGNORE INTO {table_name} 
                (device_name, record_date, voltage_level, device_type, device_code, planned_power_off_time, actual_power_off_time, planned_power_on_time, actual_power_on_time, sheet_name)
                VALUES (:device_name, :record_date, :voltage_level, :device_type, :device_code, STR_TO_DATE(:planned_power_off_time, '%Y%m%d_%H:%i:%s'), STR_TO_DATE(:actual_power_off_time, '%Y%m%d_%H:%i:%s'), STR_TO_DATE(:planned_power_on_time, '%Y%m%d_%H:%i:%s'), STR_TO_DATE(:actual_power_on_time, '%Y%m%d_%H:%i:%s'), :sheet_name)
                """)

                batch_size = 200
                for i in range(0, len(valid_records), batch_size):
                    batch = valid_records[i:i + batch_size]
                    conn.execute(insert_stmt, batch)
                    print(f"💾 已插入第 {i // batch_size + 1} 批数据 ({len(batch)} 条)")
                # 获取插入的数据总量
                count_stmt = text(f"SELECT COUNT(*) FROM {table_name} WHERE record_date = :record_date")
                count = conn.execute(count_stmt, {"record_date": data_date}).scalar()

                print(f"✅ {table_name} 数据库保存成功: {count} 条记录")
                return True, table_name, count, []
        
        except Exception as e:
            print(f"❌ 数据库保存失败: {e}")
            import traceback
            traceback.print_exc()
            return False, None, 0, []    
    def save_to_ynjichu_database(self, records, data_date):
        """保存停电数据到固定表 power_ynjichu"""
        if not records:
            print("❌ 没有可保存的记录")
            return True, None, 0, []

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
            # 添加 record_date 字段
            r["record_date"] = data_date
            valid_records.append(r)

        if not valid_records:
            print("❌ 没有可保存的有效记录")
            return True, None, 0, []

        # --- 使用固定表名 ---
        table_name = "power_jizujichu"
        preview_data = []

        try:
            with self.db_manager.engine.begin() as conn:
                # --- 创建表（如果不存在）---
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS `power_ynjichu` (
                `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键，唯一标识一条记录',
                `record_date` date NOT NULL COMMENT '记录日期',
                `unit_group_name` varchar(200) DEFAULT NULL COMMENT '机组群名（允许为空）',
                `power_plant_id` varchar(50) DEFAULT NULL COMMENT '电厂ID（允许为空）',
                `power_plant_name` varchar(200) DEFAULT NULL COMMENT '电厂名称（允许为空）',
                `unit_id` varchar(50) DEFAULT NULL COMMENT '机组ID（允许为空）',
                `unit_name` varchar(200) DEFAULT NULL COMMENT '机组名称（允许为空）',
                `proportion` decimal(10,4) DEFAULT NULL COMMENT '所占比例（允许为空，如0.35表示35%）',
                `sheet_name` varchar(255) DEFAULT NULL COMMENT '数据来源表名（允许为空）',
                `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录入库时间（自动生成）',
                `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间（自动更新）',
                PRIMARY KEY (`id`),
                KEY `idx_unit_group` (`unit_group_name`) COMMENT '机组群名索引'
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='机组群-机组分配比例记录表（所有字段允许为空）';
                """
                conn.execute(text(create_table_sql))
                print(f"✅ 表 {table_name} 已存在或创建成功")

                # 删除该日期的旧数据
                conn.execute(text(f"DELETE FROM {table_name} WHERE record_date = :record_date"), 
                             {"record_date": data_date})
                print(f"🗑️ 已删除 {data_date} 的旧数据")

                # --- 批量插入 ---
                insert_stmt = text(f"""
                INSERT IGNORE INTO {table_name} 
                (record_date, unit_group_name, power_plant_id, power_plant_name, unit_id, unit_name, proportion, sheet_name)
                VALUES 
                (:record_date, :unit_group_name, :power_plant_id, :power_plant_name, :unit_id, :unit_name, :proportion, :sheet_name)
                """)
                
                # 批量插入数据
                batch_size = 200
                for i in range(0, len(valid_records), batch_size):
                    batch = valid_records[i:i + batch_size]
                    conn.execute(insert_stmt, batch)
                    print(f"💾 已插入第 {i // batch_size + 1} 批数据 ({len(batch)} 条)")

                # 获取插入的数据总量
                count_stmt = text(f"SELECT COUNT(*) FROM {table_name} WHERE record_date = :record_date")
                count = conn.execute(count_stmt, {"record_date": data_date}).scalar()
                
                # 获取预览数据
                preview_stmt = text(f"SELECT * FROM {table_name} WHERE record_date = :record_date LIMIT 5")
                preview_result = conn.execute(preview_stmt, {"record_date": data_date})
                for row in preview_result:
                    preview_data.append(dict(row._mapping))

                print(f"✅ {table_name} 数据库保存成功: {count} 条记录")
                return True, table_name, count, []

        except Exception as e:
            print(f"❌ {table_name} 数据库保存失败: {e}")
            import traceback
            traceback.print_exc()
            return False, None, 0, []
    
    def save_to_internal_database(self, records, data_date):
        """保存发电机干预记录到固定表 generator_intervention_records"""
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
            required_fields = ["object_name", "object_id", "intervention_start_time", "intervention_end_time",
                               "pre_intervention_max", "pre_intervention_min", "post_intervention_max", "post_intervention_min",
                               "intervention_reason"]
            if not all(k in r for k in required_fields):
                continue
            r["record_date"] = data_date
            valid_records.append(r)

        if not valid_records:
            print("❌ 没有可保存的有效记录")
            return False, None, 0, []

        # --- 使用固定表名 ---
        table_name = "power_intervention"
        preview_data = []

        try:
            with self.db_manager.engine.begin() as conn:
                # --- 创建表（如果不存在） ---
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS `{table_name}` (
                  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
                  `record_date` date NOT NULL COMMENT '记录日期',
                  `sheet_name` varchar(255) DEFAULT NULL COMMENT '数据来源表名',
                  `object_name` varchar(200) NOT NULL COMMENT '对象名称（如牛远厂#2发电机）',
                  `object_id` varchar(50) NOT NULL COMMENT '对象ID（唯一标识，如40813871689367554）',
                  `intervention_start_time` datetime DEFAULT NULL COMMENT '干预开始时间（格式：YYYY-MM-DD HH:MM:SS）',
                  `intervention_end_time` datetime DEFAULT NULL COMMENT '干预结束时间（格式：YYYY-MM-DD HH:MM:SS）',
                  `pre_intervention_max` decimal(10,3) DEFAULT NULL COMMENT '干预前最大值',
                  `pre_intervention_min` decimal(10,3) DEFAULT NULL COMMENT '干预前最小值',
                  `post_intervention_max` decimal(10,3) DEFAULT NULL COMMENT '干预后最大值',
                  `post_intervention_min` decimal(10,3) DEFAULT NULL COMMENT '干预后最小值',
                  `intervention_reason` varchar(500) DEFAULT NULL COMMENT '干预原因（如配合电厂工作:优化开机曲线）',
                  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
                  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
                  PRIMARY KEY (`id`),
                  KEY `idx_object_id` (`object_id`) COMMENT '对象ID索引，用于关联查询',
                  KEY `idx_intervention_time` (`intervention_start_time`, `intervention_end_time`) COMMENT '干预时间索引，用于时间范围查询'
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='发电机干预记录信息表';
                """
                conn.execute(text(create_table_sql))
                print(f"✅ 表 {table_name} 已存在或创建成功")

                # --- 批量插入 ---
                insert_stmt = text(f"""
                INSERT IGNORE INTO {table_name} 
                (record_date, sheet_name, object_name, object_id, intervention_start_time, intervention_end_time, 
                 pre_intervention_max, pre_intervention_min, post_intervention_max, post_intervention_min, 
                 intervention_reason)
                VALUES (:record_date, :sheet_name, :object_name, :object_id, :intervention_start_time, :intervention_end_time,
                        :pre_intervention_max, :pre_intervention_min, :post_intervention_max, :post_intervention_min,
                        :intervention_reason)
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
                return True, table_name, count, []

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
        outage_records = []
        ineternal_records = []

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
            elif i in [-2]:
                outage_records = self._process_outage_as_table(df, data_date, sheet_name)
            elif i in [-1]:
                ineternal_records = self._process_internal_as_table(df, data_date, sheet_name)
            else:
                print(f"⚠️ 第{i+1}个sheet未定义处理规则，跳过")
                continue

            print(f"✅ Sheet{i+1} 处理完成，共 {len(records)} 条记录")
            all_records.extend(records)
        
        if not outage_records:
            print("❌ 没有生成任何停电记录")
            return False
        if not all_records:
            print("❌ 没有生成任何有效记录")
            return False

        success1, table_name1, count1, preview_data1 = self.save_to_database(all_records, data_date)
        success2, table_name2, count2, preview_data2 = self.save_to_outage_database(outage_records, data_date)
        success3, table_name3, count3, preview_data3 = self.save_to_internal_database(ineternal_records, data_date)
        
        # 返回两个操作的结果
        return (success1, table_name1, count1, preview_data1), (success2, table_name2, count2, preview_data2),(success3, table_name3, count3, preview_data3)
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
        if df.empty:
            print(f"警告：sheet '{sheet_name}' 无有效数据（所有行都是空行）")
            return records  # 返回空列表，避免后续报错
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
    
    def _process_3_as_channel(self, df, data_date, sheet_name):
        """
        处理设备电压等级信息sheet，提取设备电压等级数据
        """
        records = []
        df = df.dropna(how="all")  # 删除空行
        
        if df.empty:
            print(f"警告：sheet '{sheet_name}' 无有效数据（所有行都是空行）")
            return records  # 返回空列表，避免后续报错

        # 确保列名正确
        df.columns = [str(c).strip() for c in df.columns]
        
        # 检查必要的列是否存在
        required_columns = ["序号", "日期", "设备名称", "电压等级(kV)"]
        if not all(col in df.columns for col in required_columns):
            print(f"⚠️  sheet '{sheet_name}' 缺少必要的列: {required_columns}")
            return records

        # 遍历每一行数据
        for _, row in df.iterrows():
            # 跳过空行
            if pd.isna(row["序号"]) and pd.isna(row["日期"]) and pd.isna(row["设备名称"]):
                continue
                
            # 处理序号字段
            def convert_serial_number(value):
                if pd.isna(value):
                    return None
                try:
                    return int(value)
                except:
                    return None

            record = {
                "serial_number": convert_serial_number(row["序号"]),
                "record_date": data_date,  # 使用统一的日期
                "device_name": str(row["设备名称"]) if not pd.isna(row["设备名称"]) else None,
                "voltage_level": str(row["电压等级(kV)"]) if not pd.isna(row["电压等级(kV)"]) else None,
                "sheet_name": sheet_name
            }
            records.append(record)
            
        print(f"✅ Sheet '{sheet_name}' 解析完成，共 {len(records)} 条记录")
        return records

    def _process_4_as_channel(self, df, data_date, sheet_name):
        """
        处理机组基础信息sheet，提取机组群、电厂和机组信息
        """
        records = []
        df = df.dropna(how="all")  # 删除空行
        
        if df.empty:
            print(f"警告：sheet '{sheet_name}' 无有效数据（所有行都是空行）")
            return records  # 返回空列表，避免后续报错

        # 确保列名正确
        df.columns = [str(c).strip() for c in df.columns]
        
        # 检查必要的列是否存在
        required_columns = ["机组群名", "电厂ID", "电厂名称", "机组ID", "机组名称", "所占比例"]
        if not all(col in df.columns for col in required_columns):
            print(f"⚠️  sheet '{sheet_name}' 缺少必要的列: {required_columns}")
            return records

        # 遍历每一行数据
        for _, row in df.iterrows():
            # 跳过空行
            if pd.isna(row["机组群名"]) and pd.isna(row["电厂ID"]) and pd.isna(row["机组ID"]):
                continue
                
            record = {
                "record_date": data_date,
                "unit_group_name": str(row["机组群名"]) if not pd.isna(row["机组群名"]) else None,
                "power_plant_id": str(row["电厂ID"]) if not pd.isna(row["电厂ID"]) else None,
                "power_plant_name": str(row["电厂名称"]) if not pd.isna(row["电厂名称"]) else None,
                "unit_id": str(row["机组ID"]) if not pd.isna(row["机组ID"]) else None,
                "unit_name": str(row["机组名称"]) if not pd.isna(row["机组名称"]) else None,
                "proportion": float(row["所占比例"]) if not pd.isna(row["所占比例"]) else None,
                "sheet_name": sheet_name
            }
            records.append(record)
            
        print(f"✅ Sheet '{sheet_name}' 解析完成，共 {len(records)} 条记录")
        return records

    def _process_5_channel(self, df, data_date, sheet_name):
        """
        处理机组约束信息sheet，提取机组群约束配置
        """
        records = []
        df = df.dropna(how="all")  # 删除空行
        
        if df.empty:
            print(f"警告：sheet '{sheet_name}' 无有效数据（所有行都是空行）")
            return records  # 返回空列表，避免后续报错

        # 确保列名正确
        df.columns = [str(c).strip() for c in df.columns]
        
        # 检查必要的列是否存在
        required_columns = ["机组群名", "生效时间", "失效时间", "电力约束", "电量约束", "最大运行方式约束", "最小运行方式约束", "最大电量", "最小电量"]
        if not all(col in df.columns for col in required_columns):
            print(f"⚠️  sheet '{sheet_name}' 缺少必要的列: {required_columns}")
            return records

        # 遍历每一行数据
        for _, row in df.iterrows():
            # 跳过空行
            if pd.isna(row["机组群名"]) and pd.isna(row["生效时间"]) and pd.isna(row["失效时间"]):
                continue
                
            # 处理约束字段，将"是"/"否"转换为1/0
            def convert_constraint(value):
                if pd.isna(value):
                    return None
                if str(value).strip() == "是":
                    return 1
                elif str(value).strip() == "否":
                    return 0
                else:
                    return None
                    
            # 处理数值字段
            def convert_numeric(value):
                if pd.isna(value):
                    return None
                try:
                    return float(value)
                except:
                    return None

            record = {
                "record_date": data_date,
                "unit_group_name": str(row["机组群名"]) if not pd.isna(row["机组群名"]) else None,
                "effective_time": str(row["生效时间"]) if not pd.isna(row["生效时间"]) else None,
                "expire_time": str(row["失效时间"]) if not pd.isna(row["失效时间"]) else None,
                "power_constraint": convert_constraint(row["电力约束"]),
                "electricity_constraint": convert_constraint(row["电量约束"]),
                "max_operation_constraint": convert_constraint(row["最大运行方式约束"]),
                "min_operation_constraint": convert_constraint(row["最小运行方式约束"]),
                "max_electricity": convert_numeric(row["最大电量"]),
                "min_electricity": convert_numeric(row["最小电量"]),
                "sheet_name": sheet_name
            }
            records.append(record)
            
        print(f"✅ Sheet '{sheet_name}' 解析完成，共 {len(records)} 条记录")
        return records

    def _process_5_as_channel(self, df, data_date, sheet_name, data_type):
        """将时刻列名映射为channel_name"""
        records = []
        df = df.dropna(how="all")  # 删除空行
        if df.empty:
            print(f"警告：sheet '{sheet_name}' 无有效数据（所有行都是空行）")
            return records  # 返回空列表，避免后续报错
        
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
            single_match = re.search(r"\((\d{4}-\d{1,2}-\d{1,2})", file_name)
            single_data_date_str = single_match.group(1)
            single_data_date = datetime.datetime.strptime(single_data_date_str, "%Y-%m-%d").date()
            print("识别到的日期：", single_data_date)
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
            target_indexes = [0, 1, 2,3,4, 5,6,7,-5,-4,-3, -2, -1]  # 对应第1,2,4,5,6个sheet

            all_records = []
            jichu_records = []
            yueshu_records = []
            ynjichu_records = []
            jizujichu_records = []
            jizuyueshu_records = []
            ynyueshu_records = []
            shubiandian_records = []

            for i in target_indexes:
                if i >= len(sheet_names):
                    print(f"⚠️ Excel中不存在第{i+1}个sheet，跳过")
                    continue

                sheet_name = sheet_names[i]
                df = sheet_dict[sheet_name]
                print(f"\n🔹 正在处理 Sheet {i+1}: {sheet_name}")

                # 统一识别日期
                match = re.search(r"\((\d{4}-\d{1,2}-\d{1,2})", sheet_name)

                if match:
                    # 提取捕获的日期字符串并转换为date类型
                    data_date_str = match.group(1)
                    data_date = datetime.datetime.strptime(data_date_str, "%Y-%m-%d").date()
                    print("识别到的日期：", data_date)  # 输出：识别到的日期：2025-09-01（若输入是2025-09-1，会自动补0为2025-09-01）
                else:
                    print("未识别到日期格式")

                # 根据sheet序号调用不同映射函数
                if i in [0]:  # 第1个sheet：时刻→channel_name
                    records = self._process_time_as_channel(df, data_date, sheet_name, data_type)
                elif i in [1]: 
                    records = self._process_1_channel(df, data_date, sheet_name, data_type)
                elif i in [2]:  # 第3个sheet：时刻→channel_name
                    records = self._process_type_date_value(df, data_date, sheet_name, data_type)
                elif i in [3]: 
                    shubiandian_records = self._process_3_as_channel(df, data_date, sheet_name)
                elif i in [4]:  # 第4个sheet：第一行→channel_name
                    jizujichu_records = self._process_4_as_channel(df, data_date, sheet_name)
                elif i in [5]:  # 第5个sheet：时刻→channel_name
                    jizuyueshu_records = self._process_5_channel(df, data_date, sheet_name)
                elif i in [-5]:  # 第6个sheet：时刻→channel_name
                    ynyueshu_records = self._process_5_channel(df, single_data_date, sheet_name)
                elif i in [-3]:  # 第4,5个sheet：时刻→channel_name
                    records = self._process_3_channel(df, data_date, sheet_name, data_type)
                elif i in [-2, -1]:  # 第7,8个sheet：第一行→channel_name
                    records = self._process_2_channel(df, data_date, sheet_name, data_type)
                elif i in [-4,6]:  # 第9个sheet
                    records = self._process_5_as_channel(df, single_data_date, sheet_name, data_type)
                elif i in [7]:
                    ynjichu_records = self._process_4_as_channel(df, single_data_date, sheet_name)
                
                else:
                    print(f"⚠️ 第{i+1}个sheet未定义处理规则，跳过")
                    continue

                print(f"✅ Sheet{i+1} 处理完成，共 {len(records)} 条记录")
                all_records.extend(records)
               
               
            jichu_records.extend(ynjichu_records)
            jichu_records.extend(jizujichu_records)
            yueshu_records.extend(jizuyueshu_records)
            yueshu_records.extend(ynyueshu_records)
                
            if not all_records:
                print("❌ 没有生成任何有效记录")
                return False
           
            success1, table_name1, count1, preview_data1 = self.save_to_database(all_records, data_date)
            success2, table_name2, count2, preview_data2 = self.save_to_jizujichu_database(jichu_records, data_date)
            success4, table_name4, count4, preview_data4 = self.save_to_jizuyueshu_database(yueshu_records, data_date)
            success5, table_name5, count5, preview_data5 = self.save_to_shubiandian_database(shubiandian_records, data_date)

            # 返回两个操作的结果
            return (success1, table_name1, count1, preview_data1), (success2, table_name2, count2, preview_data2), (success4, table_name4, count4, preview_data4), (success5, table_name5, count5, preview_data5)

            # return self.save_to_database(all_records, data_date)
    
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
        # 首先尝试匹配括号中的日期格式 "(2025-09-29)"
        match = re.search(r"\((\d{4}-\d{2}-\d{2})\)", first_sheet_name)
        if match:
            data_date = datetime.datetime.strptime(match.group(1), "%Y-%m-%d").date()
        else:
            # 如果没有括号，则尝试直接匹配日期格式 "2025-09-29"
            match = re.search(r"(\d{4}-\d{2}-\d{2})", first_sheet_name)
            if match:
                data_date = datetime.datetime.strptime(match.group(1), "%Y-%m-%d").date()
            else:
                print(f"❌ 无法从 sheet 名称 '{first_sheet_name}' 中提取日期")
                return False, None, 0, []

        # 根据文件名识别类型
        file_name = str(excel_file)
        chinese_match = re.search(r'([\u4e00-\u9fff]+)', file_name)
        if chinese_match:
            data_type = chinese_match.group(1)
            print(f"📁 文件类型识别: {data_type}")
        else:
            print(f"⚠️ 未能在文件名中找到汉字：{file_name}，跳过。")
            return False, None, 0, []
        data_type = "广东_" + data_type
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
                        "type": "广东_"+data_type,
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
                    "channel_name": f"{data_type}_均値",
                    "value": round(overall_mean, 2),
                    "type": "广东_"+data_type,
                    "sheet_name": sheet_name,
                    "created_at": pd.Timestamp.now(),
                }
                records.append(record)

        print(f"✅ {data_type} 均値生成 {len(records)} 条記錄")
        return records

    def import_point_data_new(self, excel_file):
        """自动导入Excel第一个Sheet的数据，并按列求均值"""
        import re
        import datetime
        import pandas as pd

        try:
            xls = pd.ExcelFile(excel_file)
            first_sheet_name = xls.sheet_names[0]  # ✅ 获取第一个 sheet 名
            df = pd.read_excel(excel_file, sheet_name=first_sheet_name, header=1)
            print(f"✅ 成功读取 Excel: {excel_file}, sheet: {first_sheet_name}")
        except Exception as e:
            print(f"❌ 读取 Excel 失败: {e}")
            return False, None, 0, []
        
        # 自动识别日期
        # 首先尝试匹配括号中的日期格式 "(2025-09-29)"
        match = re.search(r"\((\d{4}-\d{2}-\d{2})\)", first_sheet_name)
        if match:
            data_date = datetime.datetime.strptime(match.group(1), "%Y-%m-%d").date()
        else:
            # 如果没有括号，则尝试直接匹配日期格式 "2025-09-29"
            match = re.search(r"(\d{4}-\d{2}-\d{2})", first_sheet_name)
            if match:
                data_date = datetime.datetime.strptime(match.group(1), "%Y-%m-%d").date()
            else:
                print(f"❌ 无法从 sheet 名称 '{first_sheet_name}' 中提取日期")
                return False, None, 0, []

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
        records = self.process_point_new(df, data_date, first_sheet_name, data_type)

        if not records:
            print("❌ 没有任何有效数据被导入")
            return False, None, 0, []

        # 保存到数据库
        success, table_name, record_count, preview_data = self.save_to_database(records, data_date)
        print(f"✅ 数据保存成功，表名: {table_name}，记录数: {record_count}")
        return success, table_name, record_count, preview_data
    
    def process_point_new(self, df, data_date, sheet_name, data_type):
        """
        针对节点电价等表格：按区域划分计算每小时均值
        """
        records = []

        # 标准化列名
        df.columns = [str(c).strip() for c in df.columns]

        # 获取时间列（第3列及之后）
        time_cols = df.columns[2:]
        if time_cols.empty or len(time_cols) == 0:
            print(f"⚠️ Sheet {sheet_name} 没有发现时间列")
            return records

        # 将时间列按每4个分组（每小时4个15分钟间隔）
        time_groups = {}
        for t in time_cols:
            hour = t.split(':')[0]
            if hour not in time_groups:
                time_groups[hour] = []
            time_groups[hour].append(t)

        # 先保存原有的数据（按小时分组）
        for _, row in df.iterrows():
            region_name = row.iloc[0]
            region_name_clean = str(region_name).strip()

            # 只处理广东和云南，排除其他地区
            if "广东" not in region_name_clean and "云南" not in region_name_clean:
                continue
                
            # 检查第一列是否有有效数据
            channel_name = row.iloc[1]
            if pd.isna(channel_name) or channel_name == "":
                continue
                
            # 为每行每小时计算均值
            for hour, times in time_groups.items():
                values = []
                for t in times:
                    value = row[t]
                    if not pd.isna(value):
                        values.append(value)
                
                if values:
                    hourly_mean = sum(values) / len(values)
                    record = {
                        "record_date": pd.to_datetime(data_date).date(),
                        "record_time": f"{hour}:00",
                        "channel_name": channel_name,
                        "value": round(hourly_mean, 2),
                        "type": region_name + "_" + data_type,
                        "sheet_name": sheet_name,
                        "created_at": pd.Timestamp.now(),
                    }
                    records.append(record)

        # 按区域分组计算每小时的均值
        region_groups = {}

        # 先按区域分组数据
        for _, row in df.iterrows():
            region_name = row.iloc[0]
            region_name_clean = str(region_name).strip()

            # 只处理广东和云南，排除其他地区
            if "广东" not in region_name_clean and "云南" not in region_name_clean:
                continue
                
            # 检查第一列是否有有效数据
            channel_name = row.iloc[1]
            if pd.isna(channel_name) or channel_name == "":
                continue
            
            # 初始化地区分组
            if region_name not in region_groups:
                region_groups[region_name] = []
            
            region_groups[region_name].append(row)

        # 为每个区域计算每小时的均值
        for region_name, region_rows in region_groups.items():
            for hour, times in time_groups.items():
                # 获取该区域所有行在这些时间点的值并计算均值
                values = []
                for t in times:
                    # 计算该时间点在该区域所有行中的均值
                    region_values = [row[t] for row in region_rows if not pd.isna(row[t])]
                    if region_values:
                        mean_value = sum(region_values) / len(region_values)
                        values.append(mean_value)
                
                # 计算4个时间点的总均值
                if values:
                    overall_mean = sum(values) / len(values)
                    record = {
                        "record_date": pd.to_datetime(data_date).date(),
                        "record_time": f"{hour}:00",
                        "channel_name": f"{data_type}_均值",
                        "value": round(overall_mean, 2),
                        "type": region_name + "_" + data_type,  # 按区域区分
                        "sheet_name": sheet_name,
                        "created_at": pd.Timestamp.now(),
                    }
                    records.append(record)

        print(f"✅ {data_type} 生成 {len(records)} 条记录")
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
                union_parts.append(f""" SELECT * FROM {table} WHERE channel_name LIKE '%均值%' AND type LIKE '%{data_type_keyword}%'""")
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

    def query_price_difference(self, date_list, region=""):
        """
        查询价差数据（日前节点电价 - 实时节点电价）
        
        Args:
            date_list (list): 日期列表，格式为 "YYYY-MM-DD"
            region (str): 地区前缀，如"云南_"，默认为空
            
        Returns:
            dict: 包含价差查询结果的字典
        """
        try:
            import pandas as pd
            
            # 构造数据类型关键词
            dayahead_keyword = f"{region}日前节点电价" if region else "日前节点电价"
            realtime_keyword = f"{region}实时节点电价" if region else "实时节点电价"
            
            print(f"🔍 查询价差数据:")
            print(f"  - 日前节点电价关键词: {dayahead_keyword}")
            print(f"  - 实时节点电价关键词: {realtime_keyword}")
            print(f"  - 日期列表: {date_list}")
            
            # 查询日前节点电价数据
            dayahead_result = self.query_daily_averages(date_list, dayahead_keyword)
            dayahead_data = dayahead_result.get("data", [])
            
            # 查询实时节点电价数据
            realtime_result = self.query_daily_averages(date_list, realtime_keyword)
            realtime_data = realtime_result.get("data", [])
            
            # 检查是否有两个数据
            if not dayahead_data:
                return {
                    "data": [],
                    "total": 0,
                    "message": f"未找到日前节点电价数据（关键词: {dayahead_keyword}）",
                    "has_dayahead": False,
                    "has_realtime": len(realtime_data) > 0
                }
            
            if not realtime_data:
                return {
                    "data": [],
                    "total": 0,
                    "message": f"未找到实时节点电价数据（关键词: {realtime_keyword}）",
                    "has_dayahead": True,
                    "has_realtime": False
                }
            
            print(f"✅ 找到日前数据: {len(dayahead_data)} 条")
            print(f"✅ 找到实时数据: {len(realtime_data)} 条")
            
            # 转换为DataFrame以便处理
            dayahead_df = pd.DataFrame(dayahead_data)
            realtime_df = pd.DataFrame(realtime_data)
            
            # 确保必要的列存在
            required_columns = ['channel_name', 'record_date', 'record_time', 'value']
            if not all(col in dayahead_df.columns for col in required_columns):
                return {
                    "data": [],
                    "total": 0,
                    "message": "日前数据缺少必要列",
                    "has_dayahead": True,
                    "has_realtime": True
                }
            
            if not all(col in realtime_df.columns for col in required_columns):
                return {
                    "data": [],
                    "total": 0,
                    "message": "实时数据缺少必要列",
                    "has_dayahead": True,
                    "has_realtime": True
                }
            
            # 统一格式化字段以便匹配
            # 1. 格式化channel_name：去除空格，统一大小写
            dayahead_df['channel_name_clean'] = dayahead_df['channel_name'].astype(str).str.strip()
            realtime_df['channel_name_clean'] = realtime_df['channel_name'].astype(str).str.strip()
            
            # 2. 格式化record_date：统一为字符串格式 YYYY-MM-DD
            def format_date(date_val):
                if pd.isna(date_val):
                    return ""
                if isinstance(date_val, str):
                    return date_val.strip()
                if hasattr(date_val, 'strftime'):
                    return date_val.strftime('%Y-%m-%d')
                return str(date_val).strip()
            
            dayahead_df['record_date_clean'] = dayahead_df['record_date'].apply(format_date)
            realtime_df['record_date_clean'] = realtime_df['record_date'].apply(format_date)
            
            # 3. 格式化record_time：统一时间格式
            def format_time(time_val):
                if pd.isna(time_val):
                    return ""
                
                # 处理timedelta对象（如 '0 days 00:00:00'）
                if hasattr(time_val, 'total_seconds'):
                    total_seconds = int(time_val.total_seconds())
                    hour = total_seconds // 3600
                    return f"{hour:02d}:00"
                
                # 如果是字符串
                if isinstance(time_val, str):
                    time_str = time_val.strip()
                    # 如果包含"days"，说明是timedelta字符串格式
                    if 'days' in time_str.lower():
                        # 解析timedelta字符串，如 "0 days 01:00:00"
                        import re
                        match = re.search(r'(\d+):(\d+):(\d+)', time_str)
                        if match:
                            hours = int(match.group(1))
                            return f"{hours:02d}:00"
                    # 如果包含冒号，直接返回
                    if ':' in time_str:
                        return time_str
                
                # 如果是数字，转换为HH:MM格式
                try:
                    if isinstance(time_val, (int, float)):
                        val = int(time_val)
                        # 如果是秒数（>=3600），转换为小时
                        if val >= 3600:
                            hour = val // 3600
                            return f"{hour:02d}:00"
                        # 如果是小时（0-23），直接使用
                        if 0 <= val < 24:
                            return f"{val:02d}:00"
                        # 如果是HHMM格式（100-2400），转换为HH:MM
                        if 100 <= val <= 2400:
                            hour = val // 100
                            return f"{hour:02d}:00"
                        # 如果是0，返回00:00
                        if val == 0:
                            return "00:00"
                except:
                    pass
                return str(time_val).strip()
            
            dayahead_df['record_time_clean'] = dayahead_df['record_time'].apply(format_time)
            realtime_df['record_time_clean'] = realtime_df['record_time'].apply(format_time)
            
            # 打印前几条数据用于调试
            print(f"📊 日前数据示例:")
            print(f"  channel_name: {dayahead_df['channel_name_clean'].head(3).tolist()}")
            print(f"  record_date: {dayahead_df['record_date_clean'].head(3).tolist()}")
            print(f"  record_time: {dayahead_df['record_time_clean'].head(3).tolist()}")
            print(f"📊 实时数据示例:")
            print(f"  channel_name: {realtime_df['channel_name_clean'].head(3).tolist()}")
            print(f"  record_date: {realtime_df['record_date_clean'].head(3).tolist()}")
            print(f"  record_time: {realtime_df['record_time_clean'].head(3).tolist()}")
            
            # 创建合并键：价差查询只使用record_date和record_time匹配
            # 因为日前和实时的channel_name可能不同（如"日前节点电价查询_均值" vs "实时节点电价查询_均值"）
            # 但如果是相同时间点的均值数据，应该匹配
            # 如果channel_name相同，也包含在合并键中；如果不同，只使用日期和时间
            dayahead_df['merge_key'] = (
                dayahead_df['record_date_clean'] + '_' +
                dayahead_df['record_time_clean']
            )
            realtime_df['merge_key'] = (
                realtime_df['record_date_clean'] + '_' +
                realtime_df['record_time_clean']
            )
            
            # 打印合并键示例
            print(f"📊 合并键示例（日前）: {dayahead_df['merge_key'].head(3).tolist()}")
            print(f"📊 合并键示例（实时）: {realtime_df['merge_key'].head(3).tolist()}")
            print(f"📊 合并键唯一值数量（日前）: {dayahead_df['merge_key'].nunique()}")
            print(f"📊 合并键唯一值数量（实时）: {realtime_df['merge_key'].nunique()}")
            
            # 合并数据
            merged_df = pd.merge(
                dayahead_df[['merge_key', 'channel_name', 'record_date', 'record_time', 'value', 'sheet_name']],
                realtime_df[['merge_key', 'value']],
                on='merge_key',
                how='inner',
                suffixes=('_dayahead', '_realtime')
            )
            
            print(f"📊 合并结果: {len(merged_df)} 条匹配记录")
            print(f"📊 日前数据唯一合并键数: {dayahead_df['merge_key'].nunique()}")
            print(f"📊 实时数据唯一合并键数: {realtime_df['merge_key'].nunique()}")
            
            if len(merged_df) == 0:
                # 提供更详细的错误信息
                dayahead_keys = set(dayahead_df['merge_key'].unique())
                realtime_keys = set(realtime_df['merge_key'].unique())
                missing_in_realtime = dayahead_keys - realtime_keys
                missing_in_dayahead = realtime_keys - dayahead_keys
                
                error_msg = "日前和实时数据无法匹配。"
                if len(missing_in_realtime) > 0:
                    error_msg += f" 日前数据中有 {len(missing_in_realtime)} 个键在实时数据中找不到（示例: {list(missing_in_realtime)[:3]}）。"
                if len(missing_in_dayahead) > 0:
                    error_msg += f" 实时数据中有 {len(missing_in_dayahead)} 个键在日前数据中找不到（示例: {list(missing_in_dayahead)[:3]}）。"
                
                return {
                    "data": [],
                    "total": 0,
                    "message": error_msg,
                    "has_dayahead": True,
                    "has_realtime": True
                }
            
            # 计算价差：两个表对应的value相减（日前节点电价 - 实时节点电价）
            # 确保value列是数值类型
            dayahead_values = pd.to_numeric(merged_df['value_dayahead'], errors='coerce')
            realtime_values = pd.to_numeric(merged_df['value_realtime'], errors='coerce')
            # 计算价差：日前 - 实时，并保留两位小数
            merged_df['value'] = (dayahead_values - realtime_values).round(2)
            
            # 将channel_name改为"价差"
            merged_df['channel_name'] = '价差'
            
            print(f"📊 价差计算示例:")
            print(f"  日前值: {dayahead_values.head(3).tolist()}")
            print(f"  实时值: {realtime_values.head(3).tolist()}")
            print(f"  价差值（保留两位小数）: {merged_df['value'].head(3).tolist()}")
            
            # 删除临时列
            merged_df = merged_df.drop(columns=['merge_key', 'value_dayahead', 'value_realtime'])
            
            # 转换为字典列表
            difference_data = merged_df.to_dict('records')
            
            print(f"✅ 价差计算完成，共 {len(difference_data)} 条记录")
            
            return {
                "data": difference_data,
                "total": len(difference_data),
                "message": "价差查询成功",
                "has_dayahead": True,
                "has_realtime": True
            }
            
        except Exception as e:
            print(f"❌ 查询价差数据失败: {str(e)}")
            import traceback
            traceback.print_exc()
            return {
                "data": [],
                "total": 0,
                "message": f"查询失败: {str(e)}",
                "has_dayahead": False,
                "has_realtime": False
            }

    def _process_outage_as_table(self, df, data_date, sheet_name):
        """将表格数据映射为停电记录，适配文件格式"""
        records = []
        df = df.dropna(how="all")  # 删除空行
        
        # 处理表头（确保列名正确映射）
        df.columns = [str(c).strip() for c in df.iloc[0]]  # 第一行作列名
        df = df[1:]  # 去掉标题行
        # 清洗列名，去除空格和特殊字符
        df.columns = [str(col).strip().replace('\n', '').replace(' ', '') for col in df.columns]
        # 验证必要列是否存在
        required_cols = ["设备名称", "电压等级", "设备类型", "设备编号", 
                        "计划停电日期", "实际停电日期", "计划复电时间", "实际复电时间"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"缺失必要列: {missing_cols}")
        
        # 遍历每一行数据
        for idx, row in df.iterrows():
            # 跳过空行和标题行（如果有残留）
            device_name = str(row.get("设备名称", "")).strip()
            if not device_name:
                continue
            
            # 构建记录字典
            record = {
                "device_name": device_name,
                "record_date": data_date,
                "sheet_name": sheet_name,
                "voltage_level": str(row.get("电压等级", "")).strip() or None,  # 空值处理为None
                "device_type": str(row.get("设备类型", "")).strip(),
                "device_code": str(row.get("设备编号", "")).strip(),
                # 时间字段保持原始格式（数据库插入时会用STR_TO_DATE转换）
                "planned_power_off_time": str(row.get("计划停电日期", "")).strip(),
                "actual_power_off_time": str(row.get("实际停电日期", "")).strip(),
                "planned_power_on_time": str(row.get("计划复电时间", "")).strip(),
                "actual_power_on_time": str(row.get("实际复电时间", "")).strip(),
            }
            
            # 验证关键字段
            if not record["device_code"]:
                print(f"跳过无效行（无设备编号）：{idx}行")
                continue
            if not all([record["planned_power_off_time"], record["planned_power_on_time"]]):
                print(f"跳过无效行（时间不完整）：{idx}行")
                continue
            
            records.append(record)
        
        return records
    
    def _process_internal_as_table(self, df, data_date, sheet_name):
        """将表格数据映射为发电机干预记录，适配文件格式"""
        records = []
        df = df.dropna(how="all")  # 删除空行
        
        # 处理表头（确保列名正确映射）
        df.columns = [str(c).strip() for c in df.iloc[0]]  # 第一行作列名
        df = df[1:]  # 去掉标题行
        # 清洗列名，去除空格和特殊字符
        df.columns = [str(col).strip().replace('\n', '').replace(' ', '') for col in df.columns]
        # 验证必要列是否存在
        required_cols = ["对象名称", "对象id", "干预开始时间", "干预结束时间", 
                        "干预前最大值", "干预前最小值", "干预后最大值", "干预后最小值", "干预原因"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"缺失必要列: {missing_cols}")
        
        # 遍历每一行数据
        for idx, row in df.iterrows():
            # 跳过空行和标题行（如果有残留）
            object_name = str(row.get("对象名称", "")).strip()
            if not object_name:
                continue
            
            # 构建记录字典
            record = {
                "record_date": data_date,
                "sheet_name": sheet_name,
                "object_name": object_name,
                "object_id": str(row.get("对象id", "")).strip(),
                "intervention_start_time": str(row.get("干预开始时间", "")).strip(),
                "intervention_end_time": str(row.get("干预结束时间", "")).strip(),
                "pre_intervention_max": row.get("干预前最大值"),
                "pre_intervention_min": row.get("干预前最小值"),
                "post_intervention_max": row.get("干预后最大值"),
                "post_intervention_min": row.get("干预后最小值"),
                "intervention_reason": str(row.get("干预原因", "")).strip(),
            }
            
            # 验证关键字段
            if not record["object_id"]:
                print(f"跳过无效行（无对象ID）：{idx}行")
                continue
            if not all([record["intervention_start_time"], record["intervention_end_time"]]):
                print(f"跳过无效行（时间不完整）：{idx}行")
                continue
            
            # 尝试转换数值字段
            try:
                for field in ["pre_intervention_max", "pre_intervention_min", "post_intervention_max", "post_intervention_min"]:
                    if record[field] is not None and str(record[field]).strip() != "":
                        record[field] = float(record[field])
                    else:
                        record[field] = None
            except ValueError as e:
                print(f"跳过无效行（数值转换失败）：{idx}行, 错误: {e}")
                continue
            
            records.append(record)
        
        return records
    
    def _process_7_channel(self, df, data_date, sheet_name):
        """将表格数据映射为机组群比例记录，适配所有字段可空的表结构"""
        records = []
        df = df.dropna(how="all")  # 删除全空行
        # 清洗列名：去除空格、换行符，确保与表字段匹配
        df.columns = [str(col).strip().replace('\n', '').replace(' ', '') for col in df.columns]
        
        # 空DataFrame校验
        if df.empty:
            print(f"警告：sheet '{sheet_name}' 无有效数据（所有行都是空行）")
            return records 
        
        # 遍历每一行数据（适配“机组群名~所占比例”表字段）
        for idx, row in df.iterrows():
            # 构建记录字典：对应表中8个业务字段，所有字段允许为空
            record = {
                "record_date": data_date,  # 外部传入的日期（如数据所属日期）
                "sheet_name": sheet_name,  # 数据来源表名
                "unit_group_name": str(row.get("机组群名", "")).strip() or None,  # 机组群名（空字符串转None）
                "power_plant_id": str(row.get("电厂ID", "")).strip() or None,    # 电厂ID
                "power_plant_name": str(row.get("电厂名称", "")).strip() or None,  # 电厂名称
                "unit_id": str(row.get("机组ID", "")).strip() or None,            # 机组ID
                "unit_name": str(row.get("机组名称", "")).strip() or None,          # 机组名称
                "proportion": row.get("所占比例"),                                 # 所占比例（数值型）
                "record_time": str(row.get("记录时间", "")).strip() or None         # 记录时间（原始格式，如20250918_15:45:00）
            }
            
            # 数值字段转换：仅处理“所占比例”，空值或非数值直接设为None（不强制校验）
            try:
                if record["proportion"] is not None and str(record["proportion"]).strip():
                    record["proportion"] = float(record["proportion"])
                else:
                    record["proportion"] = None
            except ValueError as e:
                print(f"行{idx}：'所占比例'字段非有效数值，设为None，错误：{e}")
                record["proportion"] = None
            
            # 无强制关键字段校验（所有字段可空），直接添加记录
            records.append(record)
        
        return records
    
    def save_to_shubiandian_database(self, records, data_date):
        """保存设备电压等级数据到固定表 device_voltage_level"""
        if not records:
            print("❌ 没有可保存的记录")
            return False, None, 0, []

        # 🧩 1. 如果传入的是 DataFrame，转成 list[dict]
        if isinstance(records, pd.DataFrame):
            records = records.to_dict(orient="records")

        if not isinstance(records, list):
            print(f"❌ records 类型错误: {type(records)}，应为 list[dict]")
            return False, None, 0, []

        # 🧩 2. 过滤无效记录并适配表字段
        valid_records = []
        for i, r in enumerate(records):
            if not isinstance(r, dict):
                continue
            # 添加 record_date 字段
            r["record_date"] = data_date
            valid_records.append(r)
        for i, r in enumerate(records):
            if not isinstance(r, dict):
                continue


        # --- 使用设备电压等级表的固定表名 ---
        table_name = "power_shubiandian"

        try:
            with self.db_manager.engine.begin() as conn:
                # --- 创建表（如果不存在），严格匹配设备电压等级表结构 ---
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS `{table_name}` (
                    `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键，唯一标识一条设备电压记录',
                    `record_date` date DEFAULT NULL COMMENT '日期（如2025-09-18）',
                    `device_name` varchar(300) DEFAULT NULL COMMENT '设备名称（如“110kV白沙粤溪光伏电站...开关位置”）',
                    `voltage_level` varchar(50) DEFAULT NULL COMMENT '电压等级(kV)（如“37kV”“115kV”）',
                    `sheet_name` varchar(255) DEFAULT NULL COMMENT '数据来源表名（如“设备电压等级表20250918”）',
                    `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录入库时间',
                    `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
                    PRIMARY KEY (`id`),
                    KEY `idx_device_name` (`device_name`) COMMENT '设备名称索引',
                    KEY `idx_record_date` (`record_date`) COMMENT '日期索引',
                    KEY `idx_sheet_name` (`sheet_name`) COMMENT '数据来源索引'
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
                """
                conn.execute(text(create_table_sql))

                # --- 插入数据，字段与表结构严格对应 ---
                insert_sql = text(f"""
                INSERT INTO `{table_name}` (
                    `record_date`,
                    `device_name`,
                    `voltage_level`,
                    `sheet_name`
                ) VALUES (
                    :record_date,
                    :device_name,
                    :voltage_level,
                    :sheet_name
                )
                """)
                conn.execute(insert_sql, valid_records)

                # --- 获取插入结果（预览前10条）---
                preview_sql = text(f"""
                SELECT * FROM `{table_name}`
                WHERE `record_date` = :record_date
                ORDER BY `record_date`
                LIMIT 10;
                """)
                # preview_data = conn.execute(preview_sql, {"record_date": data_date}).fetchall()

            return True, table_name, len(valid_records), []

        except Exception as e:
            print(f"保存数据时出错：{e}")
            return False, None, 0, []

    def save_to_jizuyueshu_database(self, records, data_date):
        """保存机组约束数据到固定表 unit_group_constraint"""
        if not records:
            print("❌ 没有可保存的记录")
            return True, None, 0, []

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
            # 添加 record_date 字段
            r["record_date"] = data_date
            valid_records.append(r)

        if not valid_records:
            print("❌ 没有可保存的有效记录")
            return False, None, 0, []

        # --- 使用固定表名 ---
        table_name = "power_yueshu"
        preview_data = []

        try:
            with self.db_manager.engine.begin() as conn:
                # --- 创建表（如果不存在）---
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS `{table_name}` (
                  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键，唯一标识一条约束记录',
                  `unit_group_name` varchar(200) DEFAULT NULL COMMENT '机组群名（如"东方站短路电流控制""中珠片必开机组群1"）',
                  `effective_time` datetime DEFAULT NULL COMMENT '生效时间（如2025-07-10 00:00:00，约束开始生效的时间）',
                  `expire_time` datetime DEFAULT NULL COMMENT '失效时间（如2038-01-19 11:14:07，约束失效的时间，默认长期有效）',
                  `power_constraint` tinyint(1) DEFAULT NULL COMMENT '电力约束（1=是，0=否，对应数据中的"是/否"）',
                  `electricity_constraint` tinyint(1) DEFAULT NULL COMMENT '电量约束（1=是，0=否，对应数据中的"是/否"）',
                  `max_operation_constraint` tinyint(1) DEFAULT NULL COMMENT '最大运行方式约束（1=是，0=否，对应数据中的"是/否"）',
                  `min_operation_constraint` tinyint(1) DEFAULT NULL COMMENT '最小运行方式约束（1=是，0=否，对应数据中的"是/否"）',
                  `max_electricity` decimal(18,2) DEFAULT NULL COMMENT '最大电量（数据中为0，支持小数，单位根据业务定义如MWh）',
                  `min_electricity` decimal(18,2) DEFAULT NULL COMMENT '最小电量（数据中为0，支持小数，单位同最大电量）',
                  `record_date` date DEFAULT NULL COMMENT '数据所属日期（如2025-09-18，统一标识该批数据的时间维度）',
                  `sheet_name` varchar(255) DEFAULT NULL COMMENT '数据来源表名（如"机组群约束配置表202509"，用于数据溯源）',
                  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录入库时间（自动生成，无需手动插入）',
                  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间（自动更新，无需维护）',
                  PRIMARY KEY (`id`),
                  KEY `idx_unit_group` (`unit_group_name`) COMMENT '机组群名索引，优化"按机组群查询约束"场景',
                  KEY `idx_effective_time` (`effective_time`, `expire_time`) COMMENT '生效-失效时间联合索引，优化"查询当前有效约束"场景',
                  KEY `idx_record_date` (`record_date`) COMMENT '数据日期索引，优化"按日期筛选批次数据"场景'
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='机组群约束配置表（存储机组群的电力/电量/运行方式约束配置）';
                """
                conn.execute(text(create_table_sql))
                print(f"✅ 表 {table_name} 已存在或创建成功")

                # 删除该日期的旧数据
                conn.execute(text(f"DELETE FROM {table_name} WHERE record_date = :record_date"), 
                             {"record_date": data_date})
                print(f"🗑️ 已删除 {data_date} 的旧数据")

                # --- 批量插入 ---
                insert_stmt = text(f"""
                INSERT IGNORE INTO {table_name} 
                (unit_group_name, effective_time, expire_time, power_constraint, electricity_constraint, 
                 max_operation_constraint, min_operation_constraint, max_electricity, min_electricity, 
                 record_date, sheet_name)
                VALUES 
                (:unit_group_name, :effective_time, :expire_time, :power_constraint, :electricity_constraint, 
                 :max_operation_constraint, :min_operation_constraint, :max_electricity, :min_electricity, 
                 :record_date, :sheet_name)
                """)
                
                # 批量插入数据
                batch_size = 200
                for i in range(0, len(valid_records), batch_size):
                    batch = valid_records[i:i + batch_size]
                    conn.execute(insert_stmt, batch)
                    print(f"💾 已插入第 {i // batch_size + 1} 批数据 ({len(batch)} 条)")

                # 获取插入的数据总量
                count_stmt = text(f"SELECT COUNT(*) FROM {table_name} WHERE record_date = :record_date")
                count = conn.execute(count_stmt, {"record_date": data_date}).scalar()
                
                # 获取预览数据
                preview_stmt = text(f"SELECT * FROM {table_name} WHERE record_date = :record_date LIMIT 5")
                preview_result = conn.execute(preview_stmt, {"record_date": data_date})
                for row in preview_result:
                    preview_data.append(dict(row._mapping))

                print(f"✅ {table_name} 数据库保存成功: {count} 条记录")
                return True, table_name, count, []
        except Exception as e:
            print(f"❌ {table_name} 数据库保存失败: {e}")
            import traceback
            traceback.print_exc()
            return False, None, 0, []

    def save_to_jizujichu_database(self, records, data_date):
        """保存机组基础数据到固定表 jizujichu"""
        if not records:
            print("❌ 没有可保存的记录")
            return True, None, 0, []

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
            # 添加 record_date 字段
            r["record_date"] = data_date
            valid_records.append(r)

        if not valid_records:
            print("❌ 没有可保存的有效记录")
            return False, None, 0, []

        # --- 使用固定表名 ---
        table_name = "power_jichu"
        preview_data = []

        try:
            with self.db_manager.engine.begin() as conn:
                # --- 创建表（如果不存在）---
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS `{table_name}` (
                  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键，唯一标识一条记录',
                  `unit_group_name` varchar(200) DEFAULT NULL COMMENT '机组群名（如"东方站短路电流控制""中珠片必开机组群1"）',
                  `power_plant_id` varchar(50) DEFAULT NULL COMMENT '电厂ID（唯一标识，如"0300F15000014""0300F13000059"）',
                  `power_plant_name` varchar(200) DEFAULT NULL COMMENT '电厂名称（如"沙角C厂""粤海厂"）',
                  `unit_id` varchar(100) DEFAULT NULL COMMENT '机组ID（唯一标识，如"0300F150000140HNN00FAB001"）',
                  `unit_name` varchar(100) DEFAULT NULL COMMENT '机组名称（如"C1F发电机""2G"）',
                  `proportion` decimal(5,2) DEFAULT NULL COMMENT '所占比例（数据中为整数1，支持小数如0.5表示50%，精度保留2位）',
                  `record_date` date DEFAULT NULL COMMENT '数据所属日期（如2025-09-18，统一标识数据的时间维度）',
                  `sheet_name` varchar(255) DEFAULT NULL COMMENT '数据来源表名（如"东方站机组群比例表20250918"，用于数据溯源）',
                  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录入库时间（自动生成，无需手动插入）',
                  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间（自动更新，无需维护）',
                  PRIMARY KEY (`id`),
                  KEY `idx_unit_group` (`unit_group_name`) COMMENT '机组群名索引，优化"按机组群查询所有机组"场景',
                  KEY `idx_power_plant` (`power_plant_id`, `power_plant_name`) COMMENT '电厂ID+名称联合索引，优化"按电厂筛选"场景',
                  KEY `idx_record_date` (`record_date`) COMMENT '数据日期索引，优化"按日期范围统计"场景'
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='机组群-机组分配比例记录表（存储机组群与机组的归属比例关系）';
                """
                conn.execute(text(create_table_sql))
                print(f"✅ 表 {table_name} 已存在或创建成功")

                # 删除该日期的旧数据
                conn.execute(text(f"DELETE FROM {table_name} WHERE record_date = :record_date"), 
                             {"record_date": data_date})
                print(f"🗑️ 已删除 {data_date} 的旧数据")

                # --- 批量插入 ---
                insert_stmt = text(f"""
                INSERT IGNORE INTO {table_name} 
                (unit_group_name, power_plant_id, power_plant_name, unit_id, unit_name, proportion, record_date, sheet_name)
                VALUES 
                (:unit_group_name, :power_plant_id, :power_plant_name, :unit_id, :unit_name, :proportion, :record_date, :sheet_name)
                """)
                
                # 批量插入数据
                batch_size = 200
                for i in range(0, len(valid_records), batch_size):
                    batch = valid_records[i:i + batch_size]
                    conn.execute(insert_stmt, batch)
                    print(f"💾 已插入第 {i // batch_size + 1} 批数据 ({len(batch)} 条)")

                # 获取插入的数据总量
                count_stmt = text(f"SELECT COUNT(*) FROM {table_name} WHERE record_date = :record_date")
                count = conn.execute(count_stmt, {"record_date": data_date}).scalar()
                
                # 获取预览数据
                preview_stmt = text(f"SELECT * FROM {table_name} WHERE record_date = :record_date LIMIT 5")
                preview_result = conn.execute(preview_stmt, {"record_date": data_date})
                for row in preview_result:
                    preview_data.append(dict(row._mapping))

                print(f"✅ {table_name} 数据库保存成功: {count} 条记录")
                return True, table_name, count, []

        except Exception as e:
            print(f"❌ {table_name} 数据库保存失败: {e}")
            import traceback
            traceback.print_exc()
            return False, None, 0, []
