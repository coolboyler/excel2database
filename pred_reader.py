import pandas as pd
import numpy as np
import datetime
import re
import os
from sqlalchemy import text
from database import DatabaseManager

class PowerDataImporter:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self._city_mapping = None
        self._city_mapping_loaded = False
        pass

    # ===============================
    # 城市映射相关 (节点电价 -> 城市)
    # ===============================
    _CITY_LIST_GD = [
        "广州", "深圳", "佛山", "东莞", "中山", "珠海", "江门", "惠州", "汕头", "汕尾",
        "揭阳", "潮州", "梅州", "河源", "清远", "韶关", "湛江", "茂名", "阳江", "云浮", "肇庆"
    ]
    _CITY_LIST_YN = [
        "云南", "昆明", "曲靖", "玉溪", "保山", "昭通", "丽江", "普洱", "临沧", "楚雄",
        "红河", "文山", "西双版纳", "大理", "德宏", "怒江", "迪庆"
    ]

    def _city_channel_name(self, city: str) -> str:
        return f"{city}_节点均价"

    def _normalize_node_name(self, name: str) -> str:
        if not name:
            return ""
        s = str(name).strip()
        # 去掉城市前缀
        for c in self._CITY_LIST_GD:
            if s.startswith(c):
                s = s[len(c):]
                break
        # 去掉“其他”前缀
        if s.startswith("其他"):
            s = s[2:]
        # 统一大小写/符号
        s = s.replace("ＫＶ", "kV").replace("KV", "kV").replace("kv", "kV")
        s = s.replace("＃", "#")
        # 去掉常见分隔符与单位/标识
        s = re.sub(r"[\\.·。/\\\\\\-\\s_()（）]+", "", s)
        s = s.replace("kV", "")
        s = s.replace("母线", "")
        s = s.replace("M", "").replace("m", "")
        # 仅保留汉字/数字/# 方便匹配
        s = re.sub(r"[^\u4e00-\u9fff0-9#]", "", s)
        return s

    def _extract_city_prefix(self, name: str):
        if not name:
            return None
        s = str(name).strip()
        for c in self._CITY_LIST_GD:
            if s.startswith(c):
                return c
        return None

    def _load_city_mapping(self):
        if self._city_mapping_loaded:
            return self._city_mapping or {}

        mapping = {}
        # 优先读取缓存
        base_dir = os.path.dirname(__file__)
        cache_path = os.path.join(base_dir, "state", "node_city_mapping.json")
        try:
            if os.path.exists(cache_path):
                import json
                with open(cache_path, "r", encoding="utf-8") as f:
                    mapping = json.load(f)
        except Exception:
            mapping = {}

        # 若缓存为空，尝试从 2025-06-28 文件构建
        if not mapping or len(mapping) < 200:
            candidates = [
                os.path.join(base_dir, "实时节点电价查询(2025-06-28).xlsx"),
                os.path.join(base_dir, "data", "实时节点电价查询(2025-06-28).xlsx"),
            ]
            source_path = next((p for p in candidates if os.path.exists(p)), None)
            if source_path:
                try:
                    xls = pd.ExcelFile(source_path)
                    sheet_name = xls.sheet_names[0]
                    df = pd.read_excel(source_path, sheet_name=sheet_name, usecols=[0])
                    for raw_name in df.iloc[:, 0].dropna().astype(str).tolist():
                        city = self._extract_city_prefix(raw_name)
                        if not city:
                            continue
                        key = self._normalize_node_name(raw_name)
                        if key:
                            mapping.setdefault(key, city)
                    if mapping:
                        try:
                            import json
                            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
                            with open(cache_path, "w", encoding="utf-8") as f:
                                json.dump(mapping, f, ensure_ascii=False)
                        except Exception:
                            pass
                except Exception as e:
                    print(f"⚠️ 城市映射构建失败: {e}")

        self._city_mapping = mapping
        self._city_mapping_loaded = True
        return mapping

    def _get_city_from_node(self, node_name: str):
        if not node_name:
            return None
        node_str = str(node_name).strip()
        # 明确排除云南节点，避免误映射为广东城市
        for kw in self._CITY_LIST_YN:
            if kw and kw in node_str:
                return None
        city = self._extract_city_prefix(node_str)
        if city:
            return city
        mapping = self._load_city_mapping()
        key = self._normalize_node_name(node_str)
        return mapping.get(key)

    def _extract_hour(self, time_val):
        if time_val is None or (isinstance(time_val, float) and np.isnan(time_val)):
            return None
        # datetime.time
        if hasattr(time_val, "hour"):
            try:
                return int(time_val.hour)
            except Exception:
                pass
        # timedelta
        if hasattr(time_val, "total_seconds"):
            try:
                return int(time_val.total_seconds() // 3600)
            except Exception:
                pass
        # number
        if isinstance(time_val, (int, float, np.number)) and not isinstance(time_val, bool):
            val = int(time_val)
            if val >= 3600:
                return val // 3600
            if 0 <= val < 24:
                return val
            if 100 <= val <= 2400:
                return val // 100
            if val == 0:
                return 0
        # string
        try:
            s = str(time_val).strip()
            if ":" in s:
                return int(s.split(":")[0])
            val = int(float(s))
            if val >= 3600:
                return val // 3600
            if 0 <= val < 24:
                return val
            if 100 <= val <= 2400:
                return val // 100
        except Exception:
            return None
        return None

    def ensure_city_means_for_date(self, date_str, data_type_keyword, city=None, insert=True):
        """
        为指定日期生成城市节点均价（可选插入到 power_data_YYYYMMDD）
        city=None 表示生成所有城市
        """
        try:
            date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
        except Exception:
            print(f"⚠️ 日期格式错误: {date_str}")
            return []

        table_name = f"power_data_{date_obj.strftime('%Y%m%d')}"
        existing_tables = self.db_manager.get_tables()
        if table_name not in existing_tables:
            return []

        # 查询节点记录（排除均值/城市均价行）
        type_like = f"%{data_type_keyword}%"
        sql = text(f"""
            SELECT record_time, channel_name, value, sheet_name, type
            FROM {table_name}
            WHERE type LIKE :type_like
              AND channel_name NOT LIKE '%均值%'
              AND channel_name NOT LIKE '%节点均价%'
        """)
        with self.db_manager.engine.connect() as conn:
            rows = conn.execute(sql, {"type_like": type_like}).fetchall()

        if not rows:
            return []

        # 聚合
        city_hour_values = {}
        sheet_name = None
        type_value = None
        for row in rows:
            row_dict = dict(row._mapping)
            sheet_name = sheet_name or row_dict.get("sheet_name")
            type_value = type_value or row_dict.get("type")
            node_city = self._get_city_from_node(row_dict.get("channel_name"))
            if not node_city:
                continue
            if city and node_city != city:
                continue
            hour = self._extract_hour(row_dict.get("record_time"))
            if hour is None or hour < 0 or hour > 23:
                continue
            city_hour_values.setdefault(node_city, {}).setdefault(hour, []).append(row_dict.get("value"))

        records = []
        for city_name, hour_map in city_hour_values.items():
            for hour, vals in hour_map.items():
                vals = [v for v in vals if v is not None]
                if not vals:
                    continue
                mean_val = sum(vals) / len(vals)
                records.append({
                    "record_date": date_obj,
                    "record_time": f"{hour:02d}:00",
                    "channel_name": self._city_channel_name(city_name),
                    "value": round(mean_val, 2),
                    "type": type_value or data_type_keyword,
                    "sheet_name": sheet_name or data_type_keyword,
                })

        if insert and records:
            with self.db_manager.engine.begin() as conn:
                if city:
                    conn.execute(
                        text(f"""
                            DELETE FROM {table_name}
                            WHERE record_date = :d
                              AND channel_name = :cn
                              AND type LIKE :type_like
                        """),
                        {"d": date_obj, "cn": self._city_channel_name(city), "type_like": type_like}
                    )
                else:
                    conn.execute(
                        text(f"""
                            DELETE FROM {table_name}
                            WHERE record_date = :d
                              AND channel_name LIKE '%节点均价%'
                              AND type LIKE :type_like
                        """),
                        {"d": date_obj, "type_like": type_like}
                    )
                insert_stmt = text(f"""
                    INSERT INTO {table_name}
                    (record_date, record_time, type, channel_name, value, sheet_name)
                    VALUES (:record_date, :record_time, :type, :channel_name, :value, :sheet_name)
                """)
                conn.execute(insert_stmt, records)

        return records

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

    def save_to_imformation_pred_database(self, records, data_date):
        """保存信息披露预测数据到自定义表 (动态分表)"""
        if not records:
            print("❌ 没有可保存的记录")
            return True, None, 0, []

        # 1. 过滤无效记录
        valid_records = []
        for r in records:
            if isinstance(r, dict):
                r['record_date'] = data_date
                valid_records.append(r)

        if not valid_records:
            return False, None, 0, []
        
        # 翻译映射
        translation_map = {
            "电厂名称": "power_plant_name", "机组名称": "generator_name",
            "最小技术出力": "min_technical_output", "最小技术出力(MW)": "min_technical_output",
            "额定出力": "rated_output", "额定出力(MW)": "rated_output",
            "日期": "maintenance_date", "时间": "record_time",
            # 避免与系统字段 `type` 冲突：Excel 表头“类型”映射为其它列名
            "类型": "category",
            "备注": "remarks", "序号": "seq_no", "元件名称": "component_name",
            "设备名称": "device_name", "电压等级": "voltage_level", "电压等级(Kv)": "voltage_level",
            "停电范围": "outage_scope", "停电时间": "outage_time", "送电时间": "restore_time",
            "工作内容": "work_content", "检修性质": "maintenance_type", "申请单位": "applicant",
            "数据项": "data_item", "断面名称": "section_name", "机组群名": "unit_group_name",
            "开始时间": "start_time", "结束时间": "end_time", "状态类型": "status_type",
            "设备改变原因": "equipment_change_reason",
            "机组检修预测信息": "unit_maintenance_prediction", "机组技术参数": "unit_technical_parameters",
            "检修计划": "maintenance_plan", "输变电检修预测信息": "transmission_maintenance",
            "机组检修容量预测信息": "unit_maintenance_capacity_prediction", "备用预测信息": "reserve_prediction",
            "阻塞预测信息": "congestion_prediction", "日前阻塞断面信息": "day_ahead_congestion_section",
            "必开必停机组（群）约束预测信息": "must_run_stop_unit_constraint",
            "必开必停机组信息预测信息": "must_run_stop_unit_info",
            "开停机不满足最小约束时间机组信息": "unit_constraint_violation_info",
            "必开必停容量预测信息": "must_run_stop_capacity",
            "市场机组总容量（MW）": "market_unit_total_capacity", "总容量（MW）": "total_capacity",
            "阻塞信息": "congestion_info", "报价模式": "quotation_mode", "运行日": "operation_date",
            # New mappings from user request
            "温度": "temperature", "天气": "weather", "风向": "wind_direction", "风速": "wind_speed",
            "降雨概率": "precipitation_probability", "体感温度": "apparent_temperature",
            "湿度": "humidity", "紫外线": "uv_index", "云量": "cloud_cover", "降雨量": "rainfall",
            "星期": "week_day", "天": "day",
            "统调预测": "dispatch_forecast", "A类电源预测": "class_a_power_forecast",
            "B类电源预测": "class_b_power_forecast", "地方电源预测": "local_power_forecast",
            "西电东送电源预测": "west_to_east_power_forecast", "粤港澳预测": "guangdong_hongkong_macau_forecast",
            "发电总预测": "total_generation_forecast", "现货新能源D日预测": "spot_new_energy_day_ahead_forecast",
            "统调新能源光伏预测": "dispatch_new_energy_pv_forecast", "统调新能源风电预测": "dispatch_new_energy_wind_forecast",
            "水电（含抽蓄）预测": "hydro_power_forecast_incl_pumped", "抽蓄出力预测": "pumped_storage_output_forecast",
            "实际统调负荷": "actual_dispatch_load", "A类电源实际": "actual_class_a_power",
            "B类电源实际": "actual_class_b_power", "地方电源实际": "actual_local_power",
            "西电东送实际": "actual_west_to_east_power", "粤港联络实际": "actual_guangdong_hongkong_link",
            "新能源总实际": "actual_total_new_energy", "水电含抽蓄实际": "actual_hydro_power_incl_pumped",
            "统调负荷偏差": "dispatch_load_deviation",
        }

        reserved_cols = {"id", "record_date", "sheet_name", "type", "created_at"}

        def _sanitize_identifier(name):
            # SQLAlchemy 的命名参数需要“安全”的 key（不能有 `:` 等字符），同时要避免列名过长。
            s = str(name).strip().lower()
            s = re.sub(r"[^0-9a-zA-Z_]+", "_", s)
            s = re.sub(r"_+", "_", s).strip("_")
            if not s:
                s = "col"
            if s[0].isdigit():
                s = f"c_{s}"
            # MySQL 列名最大 64 字符
            return s[:64]

        def translate(name, used=None):
            clean = str(name).strip()
            mapped = translation_map.get(clean)
            if mapped is None:
                for k, v in translation_map.items():
                    if k in clean:
                        mapped = v
                        break
            if mapped is None:
                mapped = clean

            safe = _sanitize_identifier(mapped)
            if safe in reserved_cols:
                safe = f"col_{safe}"

            if used is not None:
                base = safe
                n = 1
                while safe in used or safe in reserved_cols:
                    suffix = f"_{n}"
                    safe = (base[: (64 - len(suffix))] + suffix) if len(base) + len(suffix) > 64 else base + suffix
                    n += 1
                used.add(safe)

            return safe

        # 按 sheet 分组
        sheet_groups = {}
        for r in valid_records:
            s_name = r.get('sheet_name', 'Unknown')
            if s_name not in sheet_groups:
                sheet_groups[s_name] = []
            sheet_groups[s_name].append(r)

        preview_data = []
        
        try:
            with self.db_manager.engine.begin() as conn:
                for sheet_name, sheet_records in sheet_groups.items():
                    # 确定表名
                    base_sheet = re.sub(r'\d{4}[-/]?\d{1,2}[-/]?\d{1,2}', '', sheet_name).replace('()', '').strip()
                    table_suffix = translate(base_sheet) or "unknown"
                    table_name = f"imformation_pred_{table_suffix}".lower()
                    
                    # 确定所有列
                    all_keys = set()
                    for r in sheet_records:
                        all_keys.update(r.keys())
                    
                    # 移除系统字段以重新排序
                    if 'record_date' in all_keys: all_keys.remove('record_date')
                    if 'sheet_name' in all_keys: all_keys.remove('sheet_name')
                    if 'type' in all_keys: all_keys.remove('type')
                    if 'data_type' in all_keys: all_keys.remove('data_type')
                    if 'created_at' in all_keys: all_keys.remove('created_at')
                    
                    # 构建列定义
                    col_defs = []
                    col_map = {} # 原始列 -> 安全列
                    used_cols = set()
                    dynamic_cols = []
                    
                    for k in sorted(list(all_keys)):
                        safe_col = translate(k, used=used_cols)
                        col_map[k] = safe_col
                        comment = str(k).replace("'", "''")
                        col_defs.append(f"`{safe_col}` text COMMENT '{comment}'")
                        dynamic_cols.append(safe_col)
                        
                    # 创建表 SQL
                    dynamic_section = (",".join(col_defs) + ",") if col_defs else ""
                    create_sql = f"""
                    CREATE TABLE IF NOT EXISTS `{table_name}` (
                        `id` bigint(20) NOT NULL AUTO_INCREMENT,
                        `record_date` date DEFAULT NULL,
                        `sheet_name` varchar(255) DEFAULT NULL,
                        `type` varchar(100) DEFAULT NULL,
                        {dynamic_section}
                        `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (`id`),
                        KEY `idx_record_date` (`record_date`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                    """
                    conn.execute(text(create_sql))
                    
                    # 清理数据
                    conn.execute(text(f"DELETE FROM `{table_name}` WHERE record_date = :date"), {'date': data_date})
                    
                    # 准备插入数据
                    insert_records = []
                    insert_cols = ['record_date', 'sheet_name', 'type'] + dynamic_cols
                    for r in sheet_records:
                        new_r = {c: None for c in insert_cols}
                        new_r['record_date'] = r.get('record_date', data_date)
                        new_r['sheet_name'] = r.get('sheet_name', sheet_name)
                        new_r['type'] = r.get('type') or r.get('data_type')
                        for orig_key, safe_key in col_map.items():
                            if orig_key in r:
                                new_r[safe_key] = r.get(orig_key)
                        insert_records.append(new_r)
                        
                    # 插入
                    if insert_records:
                        keys = insert_cols
                        values_clause = ", ".join([f":{k}" for k in keys])
                        columns_clause = ", ".join([f"`{k}`" for k in keys])
                        
                        stmt = text(f"INSERT INTO `{table_name}` ({columns_clause}) VALUES ({values_clause})")
                        conn.execute(stmt, insert_records)
                        
                        print(f"✅ 已保存 {len(insert_records)} 条记录到 {table_name}")
                        if not preview_data:
                            preview_data = insert_records[:10]

            return True, None, len(valid_records), preview_data

        except Exception as e:
            print(f"❌ 保存失败: {e}")
            import traceback
            traceback.print_exc()
            return False, None, 0, []

        

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

        def _coerce_numeric(v):
            if v is None or (isinstance(v, float) and np.isnan(v)):
                return None
            if isinstance(v, (int, float, np.number)) and not isinstance(v, bool):
                return float(v)
            if isinstance(v, str):
                s = v.strip().replace(",", "")
                if not s:
                    return None
                try:
                    return float(s)
                except Exception:
                    return None
            return None

        # 🧩 2. 过滤无效记录（并保证 value 可写入 DECIMAL）
        valid_records = []
        dropped_non_numeric = 0
        for i, r in enumerate(records):
            if not isinstance(r, dict):
                continue
            required_fields = ["record_date", "record_time", "channel_name", "value", "type", "sheet_name"]
            if not all(k in r for k in required_fields):
                continue
            # 转 record_date
            if isinstance(r["record_date"], str):
                r["record_date"] = pd.to_datetime(r["record_date"]).date()
            coerced = _coerce_numeric(r.get("value"))
            if coerced is None:
                dropped_non_numeric += 1
                continue
            r["value"] = coerced
            valid_records.append(r)

        if not valid_records:
            print("❌ 没有可保存的有效记录")
            return False, None, 0, []
        if dropped_non_numeric:
            print(f"⚠️ 已跳过 {dropped_non_numeric} 条非数值 value 记录（避免写入 power_data 失败）")

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
                    preview_data.append(dict(row._mapping))
                
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
                    preview_data.append(dict(row._mapping))
                
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
            data_date = self._extract_date_from_text(sheet_name) or self._extract_date_from_text(file_name)
            if not data_date:
                print(f"⚠️ 未识别到日期: {sheet_name}，跳过")
                continue
            # 根据sheet序号调用不同映射函数
            if i in [0, 3, 4]:  # 第1,4,5个sheet：时刻→channel_name
                records = self._process_time_as_channel(df, data_date, sheet_name, data_type)
            elif i in [1, 5]:  # 第2,6个sheet：第一行→channel_name
                records = self._process_first_row_as_channel(df, data_date, sheet_name, data_type)
            elif i in [6]:
                records = self._process_fsc_as_channel(df, data_date, sheet_name, data_type)
            elif i in [-2]:
                try:
                    outage_records = self._process_outage_as_table(df, data_date, sheet_name)
                except Exception as e:
                    print(f"⚠️ 停电信息解析失败，已跳过: {e}")
                    outage_records = []
            elif i in [-1]:
                try:
                    ineternal_records = self._process_internal_as_table(df, data_date, sheet_name)
                except Exception as e:
                    print(f"⚠️ 机组内部信息解析失败，已跳过: {e}")
                    ineternal_records = []
            else:
                print(f"⚠️ 第{i+1}个sheet未定义处理规则，跳过")
                continue

            print(f"✅ Sheet{i+1} 处理完成，共 {len(records)} 条记录")
            all_records.extend(records)
        
        if not all_records:
            print("❌ 没有生成任何有效记录")
            return False

        success1, table_name1, count1, preview_data1 = self.save_to_database(all_records, data_date)
        if outage_records:
            success2, table_name2, count2, preview_data2 = self.save_to_outage_database(outage_records, data_date)
        else:
            success2, table_name2, count2, preview_data2 = True, None, 0, []
        if ineternal_records:
            success3, table_name3, count3, preview_data3 = self.save_to_internal_database(ineternal_records, data_date)
        else:
            success3, table_name3, count3, preview_data3 = True, None, 0, []
        
        # 返回两个操作的结果
        return (success1, table_name1, count1, preview_data1), (success2, table_name2, count2, preview_data2),(success3, table_name3, count3, preview_data3)

    def _extract_date_from_text(self, text_value):
        """
        从文本中提取日期（支持括号/中文括号/无括号的 YYYY-MM-DD 或 YYYYMMDD）
        返回 date 或 None
        """
        if not text_value:
            return None
        text = str(text_value)
        patterns = [
            r"[（(]\s*(\d{4}-\d{1,2}-\d{1,2})\s*[)）]",
            r"(\d{4}-\d{1,2}-\d{1,2})",
            r"(\d{8})",
        ]
        for p in patterns:
            m = re.search(p, text)
            if not m:
                continue
            s = m.group(1)
            if len(s) == 8 and s.isdigit():
                s = f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
            try:
                return datetime.datetime.strptime(s, "%Y-%m-%d").date()
            except Exception:
                continue
        return None
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
            single_data_date = self._extract_date_from_text(file_name)
            if not single_data_date:
                print(f"⚠️ 未识别到日期：{file_name}，跳过")
                return False
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
                data_date = self._extract_date_from_text(sheet_name) or single_data_date
                if data_date:
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
    
    def save_to_shubiandian_database(self, records, data_date):
        """保存输变电信息到数据库"""
        if not records:
            return True, None, 0, []
            
        table_suffix = data_date.strftime("%Y%m%d")
        table_name = f"power_substation_device_{table_suffix}"
        
        try:
            with self.db_manager.engine.begin() as conn:
                # 创建表
                create_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    serial_number INT,
                    record_date DATE,
                    device_name VARCHAR(255),
                    voltage_level VARCHAR(50),
                    sheet_name VARCHAR(255),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """
                conn.execute(text(create_sql))
                
                # 删除旧数据
                conn.execute(text(f"DELETE FROM {table_name} WHERE record_date = :d"), {"d": data_date})
                
                # 插入数据
                insert_sql = text(f"""
                INSERT INTO {table_name} (serial_number, record_date, device_name, voltage_level, sheet_name)
                VALUES (:serial_number, :record_date, :device_name, :voltage_level, :sheet_name)
                """)
                
                conn.execute(insert_sql, records)
                
                count = len(records)
                print(f"✅ {table_name} 保存成功: {count} 条")
                return True, table_name, count, records[:5]
                
        except Exception as e:
            print(f"❌ {table_name} 保存失败: {e}")
            import traceback
            traceback.print_exc()
            return False, None, 0, []

    def save_to_jizujichu_database(self, records, data_date):
        """保存机组基础信息到数据库"""
        if not records:
            return True, None, 0, []
            
        table_suffix = data_date.strftime("%Y%m%d")
        table_name = f"power_unit_basic_{table_suffix}"
        
        try:
            with self.db_manager.engine.begin() as conn:
                # 创建表
                create_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    record_date DATE,
                    unit_group_name VARCHAR(255),
                    power_plant_id VARCHAR(100),
                    power_plant_name VARCHAR(255),
                    unit_id VARCHAR(100),
                    unit_name VARCHAR(255),
                    proportion FLOAT,
                    sheet_name VARCHAR(255),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """
                conn.execute(text(create_sql))
                
                # 删除旧数据
                conn.execute(text(f"DELETE FROM {table_name} WHERE record_date = :d"), {"d": data_date})
                
                # 插入数据
                insert_sql = text(f"""
                INSERT INTO {table_name} (record_date, unit_group_name, power_plant_id, power_plant_name, unit_id, unit_name, proportion, sheet_name)
                VALUES (:record_date, :unit_group_name, :power_plant_id, :power_plant_name, :unit_id, :unit_name, :proportion, :sheet_name)
                """)
                
                conn.execute(insert_sql, records)
                
                count = len(records)
                print(f"✅ {table_name} 保存成功: {count} 条")
                return True, table_name, count, records[:5]
                
        except Exception as e:
            print(f"❌ {table_name} 保存失败: {e}")
            import traceback
            traceback.print_exc()
            return False, None, 0, []

    def save_to_jizuyueshu_database(self, records, data_date):
        """保存机组约束信息到数据库"""
        if not records:
            return True, None, 0, []
            
        table_suffix = data_date.strftime("%Y%m%d")
        table_name = f"power_unit_constraint_{table_suffix}"
        
        try:
            with self.db_manager.engine.begin() as conn:
                # 创建表
                create_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    record_date DATE,
                    unit_group_name VARCHAR(255),
                    effective_time VARCHAR(50),
                    expire_time VARCHAR(50),
                    power_constraint INT COMMENT '1=是, 0=否',
                    electricity_constraint INT COMMENT '1=是, 0=否',
                    max_operation_constraint INT COMMENT '1=是, 0=否',
                    min_operation_constraint INT COMMENT '1=是, 0=否',
                    max_electricity FLOAT,
                    min_electricity FLOAT,
                    sheet_name VARCHAR(255),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """
                conn.execute(text(create_sql))
                
                # 删除旧数据
                conn.execute(text(f"DELETE FROM {table_name} WHERE record_date = :d"), {"d": data_date})
                
                # 插入数据
                insert_sql = text(f"""
                INSERT INTO {table_name} (record_date, unit_group_name, effective_time, expire_time, 
                    power_constraint, electricity_constraint, max_operation_constraint, min_operation_constraint, 
                    max_electricity, min_electricity, sheet_name)
                VALUES (:record_date, :unit_group_name, :effective_time, :expire_time, 
                    :power_constraint, :electricity_constraint, :max_operation_constraint, :min_operation_constraint, 
                    :max_electricity, :min_electricity, :sheet_name)
                """)
                
                conn.execute(insert_sql, records)
                
                count = len(records)
                print(f"✅ {table_name} 保存成功: {count} 条")
                return True, table_name, count, records[:5]
                
        except Exception as e:
            print(f"❌ {table_name} 保存失败: {e}")
            import traceback
            traceback.print_exc()
            return False, None, 0, []

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
        file_name = os.path.basename(str(excel_file)) # 确保只取文件名
        chinese_match = re.search(r'([\u4e00-\u9fff]+)', file_name)
        if chinese_match:
            data_type = chinese_match.group(1)
            # 修正: 如果识别出的 data_type 包含 "查询" 字样，去掉它，保持简洁
            data_type = data_type.replace("查询", "")
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
        city_hour_values = {}  # {city: {hour: [values]}}
        
        for _, row in df.iterrows():
            # 检查第一列是否有有效数据，如果没有则跳过（处理标题行）
            channel_name = row.iloc[0]  # 第一列作为通道名称
            if pd.isna(channel_name) or channel_name == "":
                continue
            city_name = self._get_city_from_node(channel_name)
                
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
                    if city_name:
                        city_hour_values.setdefault(city_name, {}).setdefault(hour, []).append(hourly_mean)

        for hour, times in time_groups.items():
            values = []
            for t in times:
                mean_value = df[t].mean()
                values.append(mean_value)
            if values:
                overall_mean = sum(values) / len(values)
                record = {
                    "record_date": pd.to_datetime(data_date).date(),
                    "record_time": f"{hour}:00",
                    "channel_name": f"{data_type}_均值",
                    "value": round(overall_mean, 2),
                    "type": str(data_type),
                    "sheet_name": sheet_name,
                    "created_at": pd.Timestamp.now(),
                }
                records.append(record)

        # 生成城市节点均价
        if city_hour_values:
            for city_name, hour_map in city_hour_values.items():
                for hour, vals in hour_map.items():
                    vals = [v for v in vals if v is not None]
                    if not vals:
                        continue
                    city_mean = sum(vals) / len(vals)
                    records.append({
                        "record_date": pd.to_datetime(data_date).date(),
                        "record_time": f"{hour}:00",
                        "channel_name": self._city_channel_name(city_name),
                        "value": round(city_mean, 2),
                        "type": data_type,
                        "sheet_name": sheet_name,
                        "created_at": pd.Timestamp.now(),
                    })

        print(f"✅ {data_type} 均值生成 {len(records)} 条记录")
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
        file_name = os.path.basename(file_name)
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
    def import_imformation_true(self, excel_file):
        """自动生成的导入函数: 信息披露查询实际信息(2025-12-23).xlsx (类)"""
        try:
            sheet_dict = pd.read_excel(excel_file, sheet_name=None, header=0)
        except Exception as e:
            print(f"❌ 无法读取Excel: {e}")
            return False, None, 0, []
        file_name = str(excel_file)
        chinese_match = re.search(r'([\u4e00-\u9fff]+)', file_name)
        if chinese_match:
                data_type = chinese_match.group(1)
                print(f"📁 文件类型识别: {data_type}")
        else:
            print(f"⚠️ 未能在文件名中找到汉字：{file_name}，跳过。")
            return False
        all_records = []
        jizuchuli_records = []
        data_date = None
        
        # 尝试从文件名提取日期
        match = re.search(r'(\d{4}-\d{1,2}-\d{1,2})', str(excel_file))
        if match:
            data_date = datetime.datetime.strptime(match.group(1), '%Y-%m-%d').date()
        else:
            # 如果文件名没日期，尝试用当天或抛出警告
            print(f"⚠️ 未能在文件名中识别日期，默认使用今日")
            data_date = datetime.date.today()

        sheet_names = list(sheet_dict.keys())

        # 处理第 1 个 Sheet (原名: 负荷实际信息(2025-12-23))
        if len(sheet_names) > 0:
            current_sheet_name = sheet_names[0]
            records = self._process_imformation_true_sheet_1(sheet_dict[current_sheet_name], data_date, current_sheet_name,data_type)
            all_records.extend(records)

        # 处理第 2 个 Sheet (原名: 地方电实际信息(2025-12-23))
        if len(sheet_names) > 1:
            current_sheet_name = sheet_names[1]
            records = self._process_imformation_true_sheet_2(sheet_dict[current_sheet_name], data_date, current_sheet_name,data_type)
            all_records.extend(records)

        # 处理第 3 个 Sheet (原名: 西电东送各通道实际信息(2025-12-23))
        if len(sheet_names) > 2:
            current_sheet_name = sheet_names[2]
            records = self._process_imformation_true_sheet_3(sheet_dict[current_sheet_name], data_date, current_sheet_name,data_type)
            all_records.extend(records)


        # 处理第 5 个 Sheet (原名: 备用实际信息(2025-12-23))
        if len(sheet_names) > 4:
            current_sheet_name = sheet_names[4]
            records = self._process_imformation_true_sheet_5(sheet_dict[current_sheet_name], data_date, current_sheet_name, data_type)
            all_records.extend(records)

        # 处理第 6 个 Sheet (原名: 实时出清断面(2025-12-23))
        if len(sheet_names) > 5:
            current_sheet_name = sheet_names[5]
            records = self._process_imformation_true_sheet_6(sheet_dict[current_sheet_name], data_date, current_sheet_name,data_type)
            all_records.extend(records)

        # 处理第 7 个 Sheet (原名: 实际断面(2025-12-23))
        if len(sheet_names) > 6:
            current_sheet_name = sheet_names[6]
            records = self._process_imformation_true_sheet_7(sheet_dict[current_sheet_name], data_date, current_sheet_name,data_type)
            all_records.extend(records)

        # 处理第 9 个 Sheet (原名: 机组出力受限情况(2025-12-23))
        if len(sheet_names) > 8:
            current_sheet_name = sheet_names[8]
            records = self._process_imformation_true_sheet_9(sheet_dict[current_sheet_name], data_date, current_sheet_name)
            jizuchuli_records.extend(records)


        # 处理第 13 个 Sheet (原名: 输变电设备检修计划执行情况(2025-12-23))
        # if len(sheet_names) > 12:
        #     current_sheet_name = sheet_names[12]
        #     records = self._process_imformation_true_sheet_13(sheet_dict[current_sheet_name], data_date, current_sheet_name)
        #     all_records.extend(records)

        # 处理第 15 个 Sheet (原名: 线路停运情况(2025-12-23))
        if len(sheet_names) > 14:
            current_sheet_name = sheet_names[14]
            records = self._process_imformation_true_sheet_15(sheet_dict[current_sheet_name], data_date, current_sheet_name, data_type)
            all_records.extend(records)

        # 处理第 16 个 Sheet (原名: 机组出力情况(2025-12-23))
        if len(sheet_names) > 15:
            current_sheet_name = sheet_names[15]
            records = self._process_imformation_true_sheet_16(sheet_dict[current_sheet_name], data_date, current_sheet_name, data_type)
            all_records.extend(records)
            
        if not all_records:
            print("❌ 没有生成任何有效记录")
            return False, None, 0, []

        # 保存到数据库 (默认使用通用保存方法，可根据需要修改)
        return (self.save_to_database(all_records, data_date)),(self.save_to_generator_tech_database(jizuchuli_records, data_date))

    def _process_imformation_true_sheet_1(self, df, data_date, sheet_name,data_type):
        """自动生成的处理函数: 负荷实际信息(2025-12-23) (模式: time_series_matrix)"""
        records = []
        df = df.dropna(how='all')
        df.columns = [str(c).strip() for c in df.columns]
        
        # 识别时间列
        time_cols = [c for c in df.columns if re.match(r'^\d{1,2}:\d{2}$', c)]

        # 过滤掉“类型”等非指标行，避免通道名称被污染
        if "类型" in df.columns and "通道名称" in df.columns:
            df = df[df["类型"].astype(str).str.strip().isin(["实际"])]
        
        for _, row in df.iterrows():
            # 统一使用“通道名称”作为指标
            channel_name = str(row.get('通道名称', 'Unknown')).strip()
            
            for t in time_cols:
                val = row[t]
                if pd.isna(val): continue
                
                records.append({
                    'record_date': data_date,
                    'record_time': t,
                    'channel_name': channel_name,
                    'value': val,
                    'sheet_name': sheet_name,
                    'type': data_type,
                    'created_at': datetime.datetime.now()
                })
        return records

    def _process_imformation_true_sheet_2(self, df, data_date, sheet_name,data_type):
        """自动生成的处理函数: 地方电实际信息(2025-12-23) (模式: time_series_matrix)"""
        records = []
        df = df.dropna(how='all')
        df.columns = [str(c).strip() for c in df.columns]
        
        # 识别时间列
        time_cols = [c for c in df.columns if re.match(r'^\d{1,2}:\d{2}$', c)]

        # 过滤掉“类型”等非指标行，避免通道名称被污染
        if "类型" in df.columns and "通道名称" in df.columns:
            df = df[df["类型"].astype(str).str.strip().isin(["实际"])]
        
        for _, row in df.iterrows():
            # 假设 '类型' 列是指标名称
            channel_name = str(row.get('通道名称', 'Unknown')).strip()
            
            for t in time_cols:
                val = row[t]
                if pd.isna(val): continue
                
                records.append({
                    'record_date': data_date,
                    'record_time': t,
                    'channel_name': channel_name,
                    'value': val,
                    'sheet_name': sheet_name,
                    'type': data_type,
                    'created_at': datetime.datetime.now()
                })
        return records

    def _process_imformation_true_sheet_3(self, df, data_date, sheet_name, data_type):
        """自动生成的处理函数: 西电东送各通道实际信息(2025-12-23) (模式: time_series_matrix)"""
        records = []
        df = df.dropna(how='all')
        df.columns = [str(c).strip() for c in df.columns]
        
        # 识别时间列
        time_cols = [c for c in df.columns if re.match(r'^\d{1,2}:\d{2}$', c)]

        # 过滤掉“类型”等非指标行，避免通道名称被污染
        if "类型" in df.columns and "通道名称" in df.columns:
            df = df[df["类型"].astype(str).str.strip().isin(["实际"])]
        
        for _, row in df.iterrows():
            # 假设 '类型' 列是指标名称
            channel_name = str(row.get('通道名称', 'Unknown')).strip()
            
            for t in time_cols:
                val = row[t]
                if pd.isna(val): continue
                
                records.append({
                    'record_date': data_date,
                    'record_time': t,
                    'channel_name': channel_name,
                    'value': val,
                    'sheet_name': sheet_name,
                    'type': data_type,
                    'created_at': datetime.datetime.now()
                })
        return records
    
    def _process_imformation_true_sheet_5(self, df, data_date, sheet_name, data_type):
        """自动生成的处理函数: 备用实际信息(2025-12-23) (模式: time_series_matrix)"""
        records = []
        df = df.dropna(how='all')
        df.columns = [str(c).strip() for c in df.columns]
        
        # 识别时间列
        time_cols = [c for c in df.columns if re.match(r'^\d{1,2}:\d{2}$', c)]
        
        for _, row in df.iterrows():
            # 假设 '数据项' 列是指标名称
            channel_name = str(row.get('数据项', 'Unknown')).strip()
            
            for t in time_cols:
                val = row[t]
                if pd.isna(val): continue
                
                records.append({
                    'record_date': data_date,
                    'record_time': t,
                    'channel_name': channel_name,
                    'value': val,
                    'sheet_name': sheet_name,
                    'type': data_type,
                    'created_at': datetime.datetime.now()
                })
        return records

    def _process_imformation_true_sheet_6(self, df, data_date, sheet_name, data_type):
        """自动生成的处理函数: 实时出清断面(2025-12-23) (模式: time_series_matrix)"""
        records = []
        df = df.dropna(how='all')
        df.columns = [str(c).strip() for c in df.columns]
        
        # 识别时间列
        time_cols = [c for c in df.columns if re.match(r'^\d{1,2}:\d{2}$', c)]
        
        for _, row in df.iterrows():
            # 假设 '断面名称' 列是指标名称
            channel_name = str(row.get('断面名称', 'Unknown')).strip()
            
            for t in time_cols:
                val = row[t]
                if pd.isna(val): continue
                
                records.append({
                    'record_date': data_date,
                    'record_time': t,
                    'channel_name': channel_name,
                    'value': val,
                    'sheet_name': sheet_name,
                    'type': data_type,
                    'created_at': datetime.datetime.now()
                })
        return records

    def _process_imformation_true_sheet_7(self, df, data_date, sheet_name, data_type):
        """自动生成的处理函数: 实际断面(2025-12-23) (模式: time_series_matrix)"""
        records = []
        df = df.dropna(how='all')
        df.columns = [str(c).strip() for c in df.columns]
        
        # 识别时间列
        time_cols = [c for c in df.columns if re.match(r'^\d{1,2}:\d{2}$', c)]
        
        for _, row in df.iterrows():
            # 假设 '断面名称' 列是指标名称
            channel_name = str(row.get('断面名称', 'Unknown')).strip()
            
            for t in time_cols:
                val = row[t]
                if pd.isna(val): continue
                
                records.append({
                    'record_date': data_date,
                    'record_time': t,
                    'channel_name': channel_name,
                    'value': val,
                    'sheet_name': sheet_name,
                    'type': data_type,
                    'created_at': datetime.datetime.now()
                })
        return records
    
    def _process_imformation_true_sheet_9(self, df, data_date, sheet_name):
        """
        处理电厂机组技术参数sheet，提取机组技术参数数据，机组出力
        """
        records = []
        df = df.dropna(how="all")  # 删除空行
        
        if df.empty:
            print(f"警告：sheet '{sheet_name}' 无有效数据")
            return records

        # 确保列名正确
        df.columns = [str(c).strip().replace('（', '(').replace('）', ')') for c in df.columns]
        print(f"DEBUG: Sheet '{sheet_name}' columns: {df.columns.tolist()}")
        
        # ===== 添加调试：打印前几行原始数据 =====
        print("DEBUG: 原始数据前5行:")
        for i in range(min(5, len(df))):
            row = df.iloc[i]
            print(f"  行{i}: 电厂='{row.get('电厂名称')}' (类型: {type(row.get('电厂名称'))}), "
                f"机组='{row.get('机组名称')}', "
                f"最小出力='{row.get('最小技术出力(MW)')}', "
                f"额定出力='{row.get('额定出力(MW)')}'")
        
        # 激进的清理策略：处理电厂名称
        if "电厂名称" in df.columns:
            # 转换为字符串
            df["电厂名称"] = df["电厂名称"].astype(str)
            print("DEBUG: 转换为字符串后前5行电厂名称:")
            print(df["电厂名称"].head(5).tolist())
            
            # 将 'nan', 'None', 空白字符 替换为 NaN
            df["电厂名称"] = df["电厂名称"].replace(r'^\s*$', np.nan, regex=True)
            df["电厂名称"] = df["电厂名称"].replace(['nan', 'None'], np.nan)
            
            print("DEBUG: 替换空值后前5行电厂名称:")
            print(df["电厂名称"].head(5).tolist())
            
            # 前向填充
            df["电厂名称"] = df["电厂名称"].ffill()
            print("DEBUG: 前向填充后前5行电厂名称:")
            print(df["电厂名称"].head(5).tolist())
            
        # 遍历每一行数据
        for idx, row in df.iterrows():
            # 跳过空行
            if pd.isna(row["机组名称"]) or str(row["机组名称"]).strip() == "":
                continue
            
            # ===== 添加调试：打印当前行处理过程 =====
            print(f"\nDEBUG 处理第{idx+1}行:")
            print(f"  原始电厂名称: '{row['电厂名称']}' (类型: {type(row['电厂名称'])})")
            print(f"  原始机组名称: '{row['机组名称']}'")
            print(f"  原始最小出力: '{row['最小技术出力(MW)']}'")
            
            # 处理电厂名称
            plant_name_raw = row["电厂名称"]
            is_plant_na = pd.isna(plant_name_raw)
            plant_name_str = str(plant_name_raw).strip() if not is_plant_na else ""
            
            print(f"  pd.isna(电厂名称) = {is_plant_na}")
            print(f"  str(电厂名称) = '{plant_name_str}'")
            
            if is_plant_na or plant_name_str in ["nan", "None", ""]:
                plant_name = str(row["机组名称"]).strip()
                print(f"  → 使用机组名称作为电厂名称: '{plant_name}'")
            else:
                plant_name = plant_name_str
                print(f"  → 使用原始电厂名称: '{plant_name}'")
            
            # 处理最小出力
            min_output_raw = row["最小技术出力(MW)"]
            min_output = min_output_raw
            print(f"  原始最小出力值: {min_output_raw} (类型: {type(min_output_raw)})")
            
            if pd.isna(min_output_raw):
                min_output = 0.0
                print(f"  → 最小出力为NaN，填充为: {min_output}")
            elif str(min_output_raw) == 'None':
                min_output = 0.0
                print(f"  → 最小出力为'None'字符串，填充为: {min_output}")
            else:
                try:
                    min_output = float(min_output_raw)
                    print(f"  → 最小出力转换为浮点数: {min_output}")
                except Exception as e:
                    min_output = 0.0
                    print(f"  → 最小出力转换失败，填充为: {min_output}, 错误: {e}")
            
            record = {
                "record_date": data_date,
                "power_plant_name": plant_name,
                "generator_name": str(row["机组名称"]).strip(),
                "min_technical_output": min_output,
                "rated_output": float(row["额定出力(MW)"]) if not pd.isna(row["额定出力(MW)"]) else None,
                "sheet_name": sheet_name
            }
            
            print(f"  → 最终记录: {record}")
            records.append(record)
                
        print(f"✅ Sheet '{sheet_name}' 解析完成，共 {len(records)} 条记录")
        
        # ===== 添加调试：打印最终记录 =====
        if records and len(records) > 0:
            print("🔍 解析函数实际返回的字段名检查:")
            first_record = records[0]
            print(f"   第一条记录字段名: {list(first_record.keys())}")
            print(f"   第一条记录内容:")
            for key, value in first_record.items():
                print(f"     {key}: {repr(value)}")
            
            # 检查是否有我们期望的中文字段
            expected_fields = ["电厂名称", "机组名称", "最小技术出力(MW)", "额定出力(MW)"]
            missing_fields = [field for field in expected_fields if field not in first_record]
            if missing_fields:
                print(f"   ❗ 缺少中文字段: {missing_fields}")

        return records

    # def _process_imformation_true_sheet_13(self, df, data_date, sheet_name):
    #     """自动生成的处理函数: 输变电设备检修计划执行情况(2025-12-23) (模式: generic_table)"""
    #     records = []
    #     df = df.dropna(how='all')
    #     df.columns = [str(c).strip() for c in df.columns]
        
    #     # 通用表格处理 (直接映射所有列)
    #     for _, row in df.iterrows():
    #         record = {
    #             'record_date': data_date,
    #             'sheet_name': sheet_name,
    #             'created_at': datetime.datetime.now()
    #         }
    #         # 动态映射所有列
    #         for col in df.columns:
    #             val = row[col]
    #             if pd.notna(val):
    #                 record[col] = val
    #         records.append(record)
    #     return records

    def _process_imformation_true_sheet_15(self, df, data_date, sheet_name,data_type):
        """自动生成的处理函数: 线路停运情况(2025-12-23) (模式: generic_table)"""
        records = []
        df = df.dropna(how='all')
        df.columns = [str(c).strip() for c in df.columns]
        
        # 通用表格处理 (直接映射所有列)
        for _, row in df.iterrows():
            channel_name = str(row.get('内容', 'Unknown')).strip()
            record = {
                'record_date': data_date,
                'sheet_name': sheet_name,
                'channel_name': channel_name,
                'value': None,
                'type': data_type,
                'created_at': datetime.datetime.now()
            }
            # 动态映射所有列
            for col in df.columns:
                val = row[col]
                if pd.notna(val):
                    record[col] = val
            records.append(record)
        return records

    def _process_imformation_true_sheet_16(self, df, data_date, sheet_name, data_type):
        """自动生成的处理函数: 机组出力情况(2025-12-23) (模式: time_series_matrix)"""
        records = []
        df = df.dropna(how='all')
        df.columns = [str(c).strip() for c in df.columns]
        
        # 识别时间列
        time_cols = [c for c in df.columns if re.match(r'^\d{1,2}:\d{2}$', c)]
        
        for _, row in df.iterrows():
            # 假设 '类型' 列是指标名称
            # 优先查找 '类型'，如果没有则尝试 '数据项' (兼容性)
            channel_name = str(row.get('类型', row.get('数据项', 'Unknown'))).strip()
            
            for t in time_cols:
                val = row[t]
                if pd.isna(val): continue
                
                records.append({
                    'record_date': data_date,
                    'record_time': t,
                    'channel_name': channel_name,
                    'value': val,
                    'sheet_name': sheet_name,
                    'type': data_type,
                    'created_at': datetime.datetime.now()
                })
        return records
    
    def query_daily_averages(self, date_list, data_type_keyword="日前节点电价", station_name=None, city=None):
        """
        查询多天的均值数据（适用于已计算好的均值记录）
        
        Args:
            date_list (list): 日期列表，格式为 "YYYY-MM-DD"
            data_type_keyword (str): 数据类型关键字，用于筛选特定类型的数据
            station_name (str): 站点名称，如果提供则按照站点名称模糊匹配，否则默认匹配'均值'
            city (str): 城市名称（可选），优先于站点名称
            
        Returns:
            dict: 包含查询结果的字典
        """
        try:
            # 如果指定城市，走城市均价查询
            if city and str(city).strip():
                return self.query_city_daily_averages(date_list, data_type_keyword, city)

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
            
            # 确定筛选条件：如果有站点名称，则按站点名称模糊匹配，否则按'均值'匹配
            name_filter = f"channel_name LIKE '%{station_name}%'" if station_name and station_name.strip() else "channel_name LIKE '%均值%'"
            
            for table in valid_tables:
                union_parts.append(f""" SELECT * FROM {table} WHERE {name_filter} AND type LIKE '%{data_type_keyword}%'""")
            if not union_parts:
                return {"data": [], "total": 0, "message": "没有找到匹配的数据"}
                
            union_query = " UNION ALL ".join(union_parts)
            print(f"🚀 执行UNION查询: {union_query}")
            final_query = f"""
                SELECT * FROM ({union_query}) as combined_data
                ORDER BY record_date DESC, record_time
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

    def query_city_daily_averages(self, date_list, data_type_keyword, city):
        """
        按城市查询节点均价（会在缺失时自动按节点聚合并写回）
        """
        try:
            city = str(city).strip()
            if not city:
                return {"data": [], "total": 0, "message": "城市为空"}

            table_names = []
            for date_str in date_list:
                date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d")
                table_names.append(f"power_data_{date_obj.strftime('%Y%m%d')}")

            existing_tables = self.db_manager.get_tables()
            valid_tables = [t for t in table_names if t in existing_tables]
            if not valid_tables:
                return {"data": [], "total": 0, "message": "没有找到有效的数据表"}

            all_rows = []
            for table in valid_tables:
                # 先查已有城市均价
                type_like = f"%{data_type_keyword}%"
                city_label = self._city_channel_name(city)
                sql = text(f"""
                    SELECT * FROM {table}
                    WHERE channel_name = :cn AND type LIKE :type_like
                """)
                with self.db_manager.engine.connect() as conn:
                    rows = conn.execute(sql, {"cn": city_label, "type_like": type_like}).fetchall()
                if rows and len(rows) >= 12:
                    all_rows.extend([dict(r._mapping) for r in rows])
                    continue

                # 不足则按节点重新计算（并写回）
                date_str = table.replace("power_data_", "")
                date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
                computed = self.ensure_city_means_for_date(date_str, data_type_keyword, city=city, insert=True)
                all_rows.extend(computed)

            all_rows.sort(key=lambda r: (str(r.get("record_date", "")), str(r.get("record_time", ""))), reverse=True)
            return {
                "data": all_rows,
                "total": len(all_rows),
                "message": "查询成功"
            }
        except Exception as e:
            print(f"❌ 城市均值查询失败: {str(e)}")
            import traceback
            traceback.print_exc()
            return {"data": [], "total": 0, "message": f"查询失败: {str(e)}"}

    def query_price_difference(self, date_list, region="", station_name=None, city=None):
        """
        查询价差数据（日前节点电价 - 实时节点电价）
        
        Args:
            date_list (list): 日期列表，格式为 "YYYY-MM-DD"
            region (str): 地区前缀，如"云南_"，默认为空
            station_name (str): 站点名称
            city (str): 城市名称（可选）
            
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
            print(f"  - 站点筛选: {station_name or '默认(均值)'}")
            if city:
                print(f"  - 城市筛选: {city}")
            
            # 查询日前节点电价数据
            dayahead_result = self.query_daily_averages(date_list, dayahead_keyword, station_name, city)
            dayahead_data = dayahead_result.get("data", [])
            
            # 查询实时节点电价数据
            realtime_result = self.query_daily_averages(date_list, realtime_keyword, station_name, city)
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

    def save_to_generator_tech_database(self, records, data_date, sheet_name="机组技术参数表"):
        """
        保存电厂机组技术参数数据到数据库
        """
        if not records:
            print("❌ 没有可保存的记录")
            return False, None, 0, []

        if isinstance(records, pd.DataFrame):
            records = records.to_dict(orient="records")

        if not isinstance(records, list):
            print(f"❌ records 类型错误: {type(records)}")
            return False, None, 0, []
        
        valid_records = []
        
        for i, r in enumerate(records):
            if not isinstance(r, dict):
                continue
            
            # 🎯 使用英文字段名（与解析函数输出匹配）
            standardized_record = {
                "record_date": data_date,
                "power_plant_name": r.get("power_plant_name") or r.get("电厂名称"),
                "generator_name": r.get("generator_name") or r.get("机组名称"),
                "min_technical_output": r.get("min_technical_output") or r.get("最小技术出力(MW)"),
                "rated_output": r.get("rated_output") or r.get("额定出力(MW)"),
                "sheet_name": r.get("sheet_name") or sheet_name
            }
            
            # 🔧 关键修复：正确处理最小出力为0的情况
            min_output = standardized_record["min_technical_output"]
            
            if min_output is not None:
                # 如果是字符串类型
                if isinstance(min_output, str):
                    min_output_str = min_output.strip().lower()
                    # 处理各种0值表示
                    if min_output_str in ["0", "0.0", "0.00", "0.000", "0.0000", "0.00000"]:
                        standardized_record["min_technical_output"] = 0.0
                    elif min_output_str in ["none", "nan", "null", ""]:
                        # 新能源电站最小出力应该为0，不是None
                        standardized_record["min_technical_output"] = 0.0
                    else:
                        try:
                            # 尝试转换为浮点数
                            float_val = float(min_output)
                            standardized_record["min_technical_output"] = float_val
                        except (ValueError, TypeError):
                            # 转换失败时，新能源电站默认为0
                            standardized_record["min_technical_output"] = 0.0
                else:
                    # 已经是数字类型
                    try:
                        # 确保是浮点数
                        float_val = float(min_output)
                        standardized_record["min_technical_output"] = float_val
                    except (ValueError, TypeError):
                        standardized_record["min_technical_output"] = 0.0
            else:
                # 最小出力为None时，新能源电站默认为0
                standardized_record["min_technical_output"] = 0.0
            
            # 🔧 处理额定出力
            rated_output = standardized_record["rated_output"]
            if rated_output is not None:
                if isinstance(rated_output, str):
                    rated_str = rated_output.strip().lower()
                    if rated_str in ["none", "nan", "null", ""]:
                        standardized_record["rated_output"] = None
                    else:
                        try:
                            standardized_record["rated_output"] = float(rated_output)
                        except (ValueError, TypeError):
                            standardized_record["rated_output"] = None
                else:
                    try:
                        standardized_record["rated_output"] = float(rated_output)
                    except (ValueError, TypeError):
                        standardized_record["rated_output"] = None
            
            # 关键字段不能为空
            if not standardized_record["generator_name"]:
                continue
                
            # 🔍 调试：查看新能源电站的处理结果
            if i < 5 and standardized_record["min_technical_output"] == 0.0:
                print(f"🔍 新能源电站处理: {standardized_record['generator_name']} 最小出力设置为0.0")
                
            valid_records.append(standardized_record)

        if not valid_records:
            print("❌ 没有有效记录可保存")
            return False, None, 0, []

        table_name = "generator_technical_parameters"

        try:
            with self.db_manager.engine.begin() as conn:
                # 创建表（如果不存在）
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS `{table_name}` (
                    `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
                    `record_date` date NOT NULL COMMENT '数据日期',
                    `power_plant_name` varchar(200) NOT NULL COMMENT '电厂名称',
                    `generator_name` varchar(150) NOT NULL COMMENT '机组名称',
                    `min_technical_output` decimal(10,4) DEFAULT NULL COMMENT '最小技术出力(MW)',
                    `rated_output` decimal(10,4) DEFAULT NULL COMMENT '额定出力(MW)',
                    `sheet_name` varchar(255) DEFAULT NULL COMMENT '数据来源表名',
                    `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                    `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                    PRIMARY KEY (`id`),
                    UNIQUE KEY `uk_generator_date` (`generator_name`, `record_date`) COMMENT '机组+日期唯一索引',
                    KEY `idx_power_plant` (`power_plant_name`) COMMENT '电厂名称索引',
                    KEY `idx_record_date` (`record_date`) COMMENT '日期索引',
                    KEY `idx_sheet_name` (`sheet_name`) COMMENT '数据来源索引'
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='电厂机组技术参数表';
                """
                conn.execute(text(create_table_sql))

                # 插入数据
                insert_sql = text(f"""
                INSERT INTO `{table_name}` (
                    `record_date`,
                    `power_plant_name`,
                    `generator_name`,
                    `min_technical_output`,
                    `rated_output`,
                    `sheet_name`
                ) VALUES (
                    :record_date,
                    :power_plant_name,
                    :generator_name,
                    :min_technical_output,
                    :rated_output,
                    :sheet_name
                )
                ON DUPLICATE KEY UPDATE
                    `power_plant_name` = VALUES(power_plant_name),
                    `min_technical_output` = VALUES(min_technical_output),
                    `rated_output` = VALUES(rated_output),
                    `sheet_name` = VALUES(sheet_name),
                    `update_time` = CURRENT_TIMESTAMP
                """)
                
                # 🔍 调试：查看要插入的数据
                print("🔍 前5条要插入的数据:")
                for i, rec in enumerate(valid_records[:5]):
                    print(f"  记录{i}: {rec['generator_name']} - 最小出力: {rec['min_technical_output']} (类型: {type(rec['min_technical_output'])})")
                
                result = conn.execute(insert_sql, valid_records)
                inserted_count = result.rowcount

                # 获取插入结果预览
                preview_sql = text(f"""
                SELECT 
                    `record_date`,
                    `power_plant_name`,
                    `generator_name`,
                    `min_technical_output`,
                    `rated_output`
                FROM `{table_name}`
                WHERE `record_date` = :record_date
                ORDER BY `id` DESC
                LIMIT 5;
                """)
                
                # 获取数据并转换为普通字典列表
                preview_result = conn.execute(preview_sql, {"record_date": data_date})
                preview_data = []
                zero_min_output_count = 0
                
                for row in preview_result:
                    # 转换SQLAlchemy Row对象为普通字典
                    row_dict = {
                        "record_date": row.record_date.isoformat() if row.record_date else None,
                        "power_plant_name": row.power_plant_name,
                        "generator_name": row.generator_name,
                        "min_technical_output": float(row.min_technical_output) if row.min_technical_output is not None else None,
                        "rated_output": float(row.rated_output) if row.rated_output is not None else None
                    }
                    
                    # 统计最小出力为0的记录
                    if row.min_technical_output == 0 or row.min_technical_output == 0.0:
                        zero_min_output_count += 1
                    
                    preview_data.append(row_dict)

                print(f"✅ 成功保存 {inserted_count} 条记录到表 `{table_name}`")
                print(f"📊 统计: 最小出力为0的记录有 {zero_min_output_count} 条")
                
                if preview_data:
                    print("📊 数据预览（前5条）：")
                    for item in preview_data:
                        min_output_display = item['min_technical_output']
                        if min_output_display == 0 or min_output_display == 0.0:
                            min_output_display = "0.0"
                        print(f"   {item['power_plant_name']} - {item['generator_name']}: "
                            f"最小出力={min_output_display}MW, 额定出力={item['rated_output']}MW")

                return True, table_name, inserted_count, preview_data

        except Exception as e:
            print(f"❌ 保存数据时出错：{e}")
            import traceback
            traceback.print_exc()
            return False, None, 0, []
        
        
    def import_imformation_pred(self, excel_file):
        """自动生成的导入函数: 信息披露查询预测信息(2025-12-23).xlsx (类)"""
        try:
            sheet_dict = pd.read_excel(excel_file, sheet_name=None, header=0)
        except Exception as e:
            print(f"❌ 无法读取Excel: {e}")
            return False, None, 0, []

        power_data_records = []
        custom_table_records = []
        # “必开必停”两张表建议单独入库（保留更多维度），避免塞进 power_data 导致字段丢失且导出不清晰
        must_run_stop_group_constraint_records = []
        must_run_stop_unit_info_records = []
        data_date = None
        
        # 尝试从文件名提取日期
        match = re.search(r'(\d{4}-\d{1,2}-\d{1,2})', str(excel_file))
        if match:
            data_date = datetime.datetime.strptime(match.group(1), '%Y-%m-%d').date()
        else:
            print(f"⚠️ 未能在文件名中识别日期，默认使用今日")
            data_date = datetime.date.today()

        # 根据文件名识别类型
        file_name = str(excel_file)
        chinese_match = re.search(r'([\u4e00-\u9fff]+)', file_name)
        if chinese_match:
            data_type = chinese_match.group(1)
            print(f"📁 文件类型识别: {data_type}")
        else:
            data_type = "自动导入"
            print(f"⚠️ 未能在文件名中找到汉字，默认类型: {data_type}")

        sheet_names = list(sheet_dict.keys())

        # 通过处理函数 docstring 反向解析 sheet 名称，避免依赖 Excel 的 sheet 顺序。
        handlers_by_sheet = {}
        for attr in dir(self):
            if not attr.startswith("_process_imformation_pred_sheet_"):
                continue
            func = getattr(self, attr, None)
            if not callable(func):
                continue
            doc = (getattr(func, "__doc__", "") or "").strip()
            m = re.search(r"自动生成的处理函数:\s*(.*?)\(", doc)
            if not m:
                continue
            handlers_by_sheet[m.group(1).strip()] = func

        def resolve_handler(base_sheet_name, i):
            # 1) 精确匹配
            if base_sheet_name in handlers_by_sheet:
                return handlers_by_sheet[base_sheet_name], f"doc:{base_sheet_name}"
            # 2) 模糊匹配：取最长匹配的 key
            best_func, best_key = None, None
            for k, f in handlers_by_sheet.items():
                if k and (k in base_sheet_name or base_sheet_name in k):
                    if best_key is None or len(k) > len(best_key):
                        best_key, best_func = k, f
            if best_func:
                return best_func, f"fuzzy:{best_key}"
            # 3) 回退：仍然按索引调用（兼容旧生成逻辑）
            func_name = f"_process_imformation_pred_sheet_{i+1}"
            if hasattr(self, func_name):
                return getattr(self, func_name), f"index:{i+1}"
            return None, None

        def _looks_like_time_series_numeric(records):
            # 时序数据必须满足：有 record_time/value，且 value 大概率为数值（避免把文本型时序塞进 power_data.value DECIMAL）。
            if not records:
                return False
            first = records[0]
            if 'record_time' not in first or 'value' not in first:
                return False
            rt = first.get('record_time')
            if not (rt and isinstance(rt, str) and ':' in rt):
                return False

            sample = records[:50]
            ok = 0
            total = 0
            for r in sample:
                v = r.get('value')
                if v is None or (isinstance(v, float) and np.isnan(v)):
                    continue
                total += 1
                if isinstance(v, (int, float, np.number)) and not isinstance(v, bool):
                    ok += 1
                    continue
                if isinstance(v, str):
                    s = v.strip().replace(",", "")
                    try:
                        float(s)
                        ok += 1
                    except Exception:
                        pass
            # 如果采样里大部分非空 value 可转成数值，则按时序写入 power_data
            return total > 0 and (ok / total) >= 0.9

        # 动态处理所有 Sheet，根据内容模式分发
        for i, sheet_name in enumerate(sheet_names):
            base_sheet_name = re.sub(r'\(\d{4}[-/]?\d{1,2}[-/]?\d{1,2}\)', '', str(sheet_name))
            base_sheet_name = re.sub(r'\d{4}[-/]?\d{1,2}[-/]?\d{1,2}', '', base_sheet_name).strip()

            # 必开必停（群）约束：包含机组群/电厂/机组/数据类型 + 15分钟曲线（值可能是台数/容量等）
            if base_sheet_name == "必开必停机组（群）约束预测信息":
                try:
                    must_run_stop_group_constraint_records.extend(
                        self._process_must_run_stop_group_constraint_sheet(
                            sheet_dict[sheet_name], data_date, sheet_name, data_type
                        )
                    )
                except Exception as e:
                    print(f"⚠️ 处理 Sheet '{sheet_name}' (必开必停机组（群）约束预测信息) 时出错: {e}")
                continue

            # 必开必停机组信息：通常为“标签/类型/原因”等文本时序（15分钟）
            if base_sheet_name == "必开必停机组信息预测信息":
                try:
                    must_run_stop_unit_info_records.extend(
                        self._process_must_run_stop_unit_info_sheet(
                            sheet_dict[sheet_name], data_date, sheet_name, data_type
                        )
                    )
                except Exception as e:
                    print(f"⚠️ 处理 Sheet '{sheet_name}' (必开必停机组信息预测信息) 时出错: {e}")
                continue

            func, match_reason = resolve_handler(base_sheet_name, i)
            if not func:
                print(f"⚠️ 未找到处理 Sheet '{sheet_name}' (基础名 '{base_sheet_name}') 的函数")
                continue

            try:
                records = func(sheet_dict[sheet_name], data_date, sheet_name, data_type)

                # 智能分发逻辑
                if records:
                    if _looks_like_time_series_numeric(records):
                        power_data_records.extend(records)
                    else:
                        custom_table_records.extend(records)

            except Exception as e:
                print(f"⚠️ 处理 Sheet '{sheet_name}' (匹配 {match_reason}) 时出错: {e}")

        if not power_data_records and not custom_table_records:
            # 允许“只有必开必停两张表”也能入库
            if not must_run_stop_group_constraint_records and not must_run_stop_unit_info_records:
                print("❌ 没有生成任何有效记录")
                return False, None, 0, []

        results = []
        
        # 1. 保存时序数据到 power_data
        if power_data_records:
            print(f"📊 保存 {len(power_data_records)} 条时序数据到 power_data")
            res_power = self.save_to_database(power_data_records, data_date)
            results.append(res_power)
            
        # 2. 保存其他数据到自定义表
        if custom_table_records:
            print(f"📊 保存 {len(custom_table_records)} 条自定义数据到独立表")
            res_custom = self.save_to_imformation_pred_database(custom_table_records, data_date)
            results.append(res_custom)

        # 3. 保存“必开必停机组（群）约束预测信息”
        if must_run_stop_group_constraint_records:
            print(f"📊 保存 {len(must_run_stop_group_constraint_records)} 条必开必停机组（群）约束数据到独立表")
            res_mrsc = self.save_must_run_stop_group_constraint_ts(must_run_stop_group_constraint_records, data_date)
            results.append(res_mrsc)

        # 4. 保存“必开必停机组信息预测信息”
        if must_run_stop_unit_info_records:
            print(f"📊 保存 {len(must_run_stop_unit_info_records)} 条必开必停机组信息数据到独立表")
            res_mrui = self.save_must_run_stop_unit_info_ts(must_run_stop_unit_info_records, data_date)
            results.append(res_mrui)

        return tuple(results) if len(results) > 1 else (results[0] if results else False)

    def _process_must_run_stop_group_constraint_sheet(self, df, data_date, sheet_name, data_type):
        """
        解析“必开必停机组（群）约束预测信息”：
        - 元数据列：机组群名/机组台数/电厂ID/电厂名称/机组ID/机组名称/数据类型
        - 15分钟时间列：00:00..23:45
        目标：保留维度，按 long 表入库，便于导出/透视。
        """
        records = []
        df = df.dropna(how="all")
        df.columns = [str(c).strip() for c in df.columns]

        time_cols = [c for c in df.columns if re.match(r"^\d{2}:\d{2}$", str(c))]
        if not time_cols:
            return records

        def _to_int(v):
            if v is None or (isinstance(v, float) and np.isnan(v)):
                return None
            if isinstance(v, (int, np.integer)) and not isinstance(v, bool):
                return int(v)
            if isinstance(v, float):
                if np.isnan(v):
                    return None
                return int(v)
            s = str(v).strip()
            if not s:
                return None
            try:
                return int(float(s.replace(",", "")))
            except Exception:
                return None

        def _to_float(v):
            if v is None or (isinstance(v, float) and np.isnan(v)):
                return None
            if isinstance(v, (int, float, np.number)) and not isinstance(v, bool):
                return float(v)
            s = str(v).strip()
            if not s:
                return None
            try:
                return float(s.replace(",", ""))
            except Exception:
                return None

        for _, row in df.iterrows():
            unit_group_name = str(row.get("机组群名", "")).strip()
            if not unit_group_name:
                continue

            rec_base = {
                "record_date": data_date,
                "sheet_name": sheet_name,
                "type": data_type,
                "unit_group_name": unit_group_name,
                "unit_count": _to_int(row.get("机组台数")),
                "plant_id": _to_int(row.get("电厂ID")),
                "plant_name": str(row.get("电厂名称", "")).strip() or None,
                "unit_id": _to_int(row.get("机组ID")),
                "unit_name": str(row.get("机组名称", "")).strip() or None,
                "constraint_type": str(row.get("数据类型", "")).strip() or None,
            }

            for t in time_cols:
                v = row.get(t)
                if pd.isna(v):
                    continue

                v_num = _to_float(v)
                v_text = None if v_num is not None else str(v).strip()

                r = dict(rec_base)
                r["record_time"] = str(t).strip()
                r["value_num"] = v_num
                r["value_text"] = v_text
                records.append(r)

        return records

    def _process_must_run_stop_unit_info_sheet(self, df, data_date, sheet_name, data_type):
        """
        解析“必开必停机组信息预测信息”：
        - 元数据列：电厂名称/机组名称/数据类型（标签/类型/原因...）
        - 15分钟时间列：00:00..23:45
        值通常为文本（必开/必停/原因等），按 long 表入库。
        """
        records = []
        df = df.dropna(how="all")
        df.columns = [str(c).strip() for c in df.columns]

        time_cols = [c for c in df.columns if re.match(r"^\d{2}:\d{2}$", str(c))]
        if not time_cols:
            return records

        for _, row in df.iterrows():
            plant_name = str(row.get("电厂名称", "")).strip()
            unit_name = str(row.get("机组名称", "")).strip()
            row_type = str(row.get("数据类型", "")).strip()
            if not (plant_name or unit_name):
                continue

            base = {
                "record_date": data_date,
                "sheet_name": sheet_name,
                "type": data_type,
                "plant_name": plant_name or None,
                "unit_name": unit_name or None,
                "row_type": row_type or None,
            }

            for t in time_cols:
                v = row.get(t)
                if pd.isna(v):
                    continue

                r = dict(base)
                r["record_time"] = str(t).strip()
                r["value_text"] = str(v).strip()
                records.append(r)

        return records

    def save_must_run_stop_group_constraint_ts(self, records, data_date):
        """入库：必开必停机组（群）约束预测信息（15分钟 long 表）。"""
        table_name = "info_disclose_pred_must_run_stop_group_constraint_ts"
        preview_data = []

        try:
            with self.db_manager.engine.begin() as conn:
                conn.execute(
                    text(
                        f"""
                        CREATE TABLE IF NOT EXISTS `{table_name}` (
                          `id` BIGINT NOT NULL AUTO_INCREMENT,
                          `record_date` DATE NOT NULL,
                          `record_time` TIME NOT NULL,
                          `unit_group_name` VARCHAR(255) NULL,
                          `unit_count` INT NULL,
                          `plant_id` INT NULL,
                          `plant_name` VARCHAR(255) NULL,
                          `unit_id` INT NULL,
                          `unit_name` VARCHAR(255) NULL,
                          `constraint_type` VARCHAR(255) NULL,
                          `value_num` DECIMAL(18,4) NULL,
                          `value_text` VARCHAR(255) NULL,
                          `sheet_name` VARCHAR(255) NULL,
                          `type` VARCHAR(255) NULL,
                          `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                          PRIMARY KEY (`id`),
                          KEY `idx_record_date` (`record_date`),
                          KEY `idx_group` (`record_date`, `unit_group_name`(64))
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                        """
                    )
                )

                # 同一天重导：删除旧数据再插入（避免重复）
                conn.execute(text(f"DELETE FROM `{table_name}` WHERE record_date = :d"), {"d": data_date})

                if not records:
                    return True, table_name, 0, []

                cols = [
                    "record_date",
                    "record_time",
                    "unit_group_name",
                    "unit_count",
                    "plant_id",
                    "plant_name",
                    "unit_id",
                    "unit_name",
                    "constraint_type",
                    "value_num",
                    "value_text",
                    "sheet_name",
                    "type",
                ]
                stmt = text(
                    f"INSERT INTO `{table_name}` ({', '.join('`'+c+'`' for c in cols)}) "
                    f"VALUES ({', '.join(':'+c for c in cols)})"
                )

                batch_size = 500
                for i in range(0, len(records), batch_size):
                    conn.execute(stmt, records[i : i + batch_size])

                preview_data = records[:10]

            return True, table_name, len(records), preview_data
        except Exception as e:
            print(f"❌ 保存必开必停机组（群）约束数据失败: {e}")
            import traceback
            traceback.print_exc()
            return False, table_name, 0, []

    def save_must_run_stop_unit_info_ts(self, records, data_date):
        """入库：必开必停机组信息预测信息（15分钟 long 表，文本为主）。"""
        table_name = "info_disclose_pred_must_run_stop_unit_info_ts"
        preview_data = []

        try:
            with self.db_manager.engine.begin() as conn:
                conn.execute(
                    text(
                        f"""
                        CREATE TABLE IF NOT EXISTS `{table_name}` (
                          `id` BIGINT NOT NULL AUTO_INCREMENT,
                          `record_date` DATE NOT NULL,
                          `record_time` TIME NOT NULL,
                          `plant_name` VARCHAR(255) NULL,
                          `unit_name` VARCHAR(255) NULL,
                          `row_type` VARCHAR(255) NULL,
                          `value_text` TEXT NULL,
                          `sheet_name` VARCHAR(255) NULL,
                          `type` VARCHAR(255) NULL,
                          `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                          PRIMARY KEY (`id`),
                          KEY `idx_record_date` (`record_date`),
                          KEY `idx_unit` (`record_date`, `plant_name`(64), `unit_name`(64))
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                        """
                    )
                )

                conn.execute(text(f"DELETE FROM `{table_name}` WHERE record_date = :d"), {"d": data_date})

                if not records:
                    return True, table_name, 0, []

                cols = [
                    "record_date",
                    "record_time",
                    "plant_name",
                    "unit_name",
                    "row_type",
                    "value_text",
                    "sheet_name",
                    "type",
                ]
                stmt = text(
                    f"INSERT INTO `{table_name}` ({', '.join('`'+c+'`' for c in cols)}) "
                    f"VALUES ({', '.join(':'+c for c in cols)})"
                )

                batch_size = 500
                for i in range(0, len(records), batch_size):
                    conn.execute(stmt, records[i : i + batch_size])

                preview_data = records[:10]

            return True, table_name, len(records), preview_data
        except Exception as e:
            print(f"❌ 保存必开必停机组信息数据失败: {e}")
            import traceback
            traceback.print_exc()
            return False, table_name, 0, []

    def _process_imformation_pred_sheet_1(self, df, data_date, sheet_name, data_type):
        """自动生成的处理函数: 负荷预测信息(2025-12-23) (模式: time_series_matrix)"""
        records = []
        df = df.dropna(how='all')
        df.columns = [str(c).strip() for c in df.columns]
        
        # 识别时间列
        time_cols = [c for c in df.columns if re.match(r'^\d{1,2}:\d{2}$', c)]

        # 过滤掉“类型”等非指标行，避免通道名称被污染
        if "类型" in df.columns and "通道名称" in df.columns:
            df = df[df["类型"].astype(str).str.strip().isin(["预测"])]
        
        for _, row in df.iterrows():
            # 假设 '类型' 列是指标名称
            channel_name = str(row.get('通道名称', 'Unknown')).strip()
            
            for t in time_cols:
                val = row[t]
                if pd.isna(val): continue
                
                records.append({
                    'record_date': data_date,
                    'record_time': t,
                    'channel_name': channel_name,
                    'value': val,
                    'sheet_name': sheet_name,
                    'type': data_type,
                    'created_at': datetime.datetime.now()
                })
        return records

    def _process_imformation_pred_sheet_2(self, df, data_date, sheet_name, data_type):
        """自动生成的处理函数: 地方电预测信息(2025-12-23) (模式: time_series_matrix)"""
        records = []
        df = df.dropna(how='all')
        df.columns = [str(c).strip() for c in df.columns]
        
        # 识别时间列
        time_cols = [c for c in df.columns if re.match(r'^\d{1,2}:\d{2}$', c)]

        # 过滤掉“类型”等非指标行，避免通道名称被污染
        if "类型" in df.columns and "通道名称" in df.columns:
            df = df[df["类型"].astype(str).str.strip().isin(["预测"])]
        
        for _, row in df.iterrows():
            # 假设 '类型' 列是指标名称
            channel_name = str(row.get('通道名称', 'Unknown')).strip()
            
            for t in time_cols:
                val = row[t]
                if pd.isna(val): continue
                
                records.append({
                    'record_date': data_date,
                    'record_time': t,
                    'channel_name': channel_name,
                    'value': val,
                    'sheet_name': sheet_name,
                    'type': data_type,
                    'created_at': datetime.datetime.now()
                })
        return records

    def _process_imformation_pred_sheet_3(self, df, data_date, sheet_name, data_type):
        """自动生成的处理函数: 发电总出力预测信息(2025-12-23) (模式: time_series_matrix)"""
        records = []
        df = df.dropna(how='all')
        df.columns = [str(c).strip() for c in df.columns]
        
        # 识别时间列
        time_cols = [c for c in df.columns if re.match(r'^\d{1,2}:\d{2}$', c)]
        
        for _, row in df.iterrows():
            # 假设 '类型' 列是指标名称
            channel_name = str(row.get('类型', 'Unknown')).strip()
            
            for t in time_cols:
                val = row[t]
                if pd.isna(val): continue
                
                records.append({
                    'record_date': data_date,
                    'record_time': t,
                    'channel_name': channel_name,
                    'value': val,
                    'sheet_name': sheet_name,
                    'type': data_type,
                    'created_at': datetime.datetime.now()
                })
        return records

    def _process_imformation_pred_sheet_4(self, df, data_date, sheet_name, data_type):
        """自动生成的处理函数: 现货新能源总出力(2025-12-23) (模式: time_series_matrix)"""
        records = []
        df = df.dropna(how='all')
        df.columns = [str(c).strip() for c in df.columns]
        
        # 识别时间列
        time_cols = [c for c in df.columns if re.match(r'^\d{1,2}:\d{2}$', c)]
        
        for _, row in df.iterrows():
            # 假设 '类型' 列是指标名称
            channel_name = str(row.get('类型', 'Unknown')).strip()
            
            for t in time_cols:
                val = row[t]
                if pd.isna(val): continue
                
                records.append({
                    'record_date': data_date,
                    'record_time': t,
                    'channel_name': channel_name,
                    'value': val,
                    'sheet_name': sheet_name,
                    'type': data_type,
                    'created_at': datetime.datetime.now()
                })
        return records

    def _process_imformation_pred_sheet_5(self, df, data_date, sheet_name, data_type):
        """自动生成的处理函数: 统调新能源出力信息(2025-12-23) (模式: time_series_matrix)"""
        records = []
        df = df.dropna(how='all')
        df.columns = [str(c).strip() for c in df.columns]
        
        # 识别时间列
        time_cols = [c for c in df.columns if re.match(r'^\d{1,2}:\d{2}$', c)]
        
        for _, row in df.iterrows():
            # 假设 '类型' 列是指标名称
            channel_name = str(row.get('类型', 'Unknown')).strip()
            
            for t in time_cols:
                val = row[t]
                if pd.isna(val): continue
                
                records.append({
                    'record_date': data_date,
                    'record_time': t,
                    'channel_name': channel_name,
                    'value': val,
                    'sheet_name': sheet_name,
                    'type': data_type,
                    'created_at': datetime.datetime.now()
                })
        return records

    def _process_imformation_pred_sheet_6(self, df, data_date, sheet_name, data_type):
        """自动生成的处理函数: 水电（含抽蓄）总出力预测信息(2025-12-23) (模式: time_series_matrix)"""
        records = []
        df = df.dropna(how='all')
        df.columns = [str(c).strip() for c in df.columns]
        
        # 识别时间列
        time_cols = [c for c in df.columns if re.match(r'^\d{1,2}:\d{2}$', c)]
        
        for _, row in df.iterrows():
            # 假设 '类型' 列是指标名称
            channel_name = str(row.get('类型', 'Unknown')).strip()
            
            for t in time_cols:
                val = row[t]
                if pd.isna(val): continue
                
                records.append({
                    'record_date': data_date,
                    'record_time': t,
                    'channel_name': channel_name,
                    'value': val,
                    'sheet_name': sheet_name,
                    'type': data_type,
                    'created_at': datetime.datetime.now()
                })
        return records

    def _process_imformation_pred_sheet_7(self, df, data_date, sheet_name, data_type):
        """自动生成的处理函数: 抽蓄电站出力计划(2025-12-23) (模式: time_series_matrix)"""
        records = []
        df = df.dropna(how='all')
        df.columns = [str(c).strip() for c in df.columns]
        
        # 识别时间列
        time_cols = [c for c in df.columns if re.match(r'^\d{1,2}:\d{2}$', c)]
        
        for _, row in df.iterrows():
            # 假设 '类型' 列是指标名称
            channel_name = str(row.get('类型', 'Unknown')).strip()
            
            for t in time_cols:
                val = row[t]
                if pd.isna(val): continue
                
                records.append({
                    'record_date': data_date,
                    'record_time': t,
                    'channel_name': channel_name,
                    'value': val,
                    'sheet_name': sheet_name,
                    'type': data_type,
                    'created_at': datetime.datetime.now()
                })
        return records

    def _process_imformation_pred_sheet_8(self, df, data_date, sheet_name, data_type):
        """自动生成的处理函数: 机组检修预测信息(2025-12-23) (模式: generic_table)"""
        records = []
        df = df.dropna(how='all')
        df.columns = [str(c).strip() for c in df.columns]
        
        # 通用表格处理 (直接映射所有列)
        for _, row in df.iterrows():
            record = {
                'record_date': data_date,
                'sheet_name': sheet_name,
                'type': data_type,
                'created_at': datetime.datetime.now()
            }
            # 动态映射所有列
            for col in df.columns:
                val = row[col]
                if pd.notna(val):
                    record[col] = val
            records.append(record)
        return records

    def _process_imformation_pred_sheet_9(self, df, data_date, sheet_name, data_type):
        """自动生成的处理函数: 输变电检修预测信息(2025-12-23) (模式: generic_table)"""
        records = []
        df = df.dropna(how='all')
        df.columns = [str(c).strip() for c in df.columns]
        
        # 通用表格处理 (直接映射所有列)
        for _, row in df.iterrows():
            record = {
                'record_date': data_date,
                'sheet_name': sheet_name,
                'type': data_type,
                'created_at': datetime.datetime.now()
            }
            # 动态映射所有列
            for col in df.columns:
                val = row[col]
                if pd.notna(val):
                    record[col] = val
            records.append(record)
        return records

    def _process_imformation_pred_sheet_10(self, df, data_date, sheet_name, data_type):
        """自动生成的处理函数: 机组检修容量预测信息(2025-12-23) (模式: generic_table)"""
        records = []
        df = df.dropna(how='all')
        df.columns = [str(c).strip() for c in df.columns]
        
        # 通用表格处理 (直接映射所有列)
        for _, row in df.iterrows():
            record = {
                'record_date': data_date,
                'sheet_name': sheet_name,
                'type': data_type,
                'created_at': datetime.datetime.now()
            }
            # 动态映射所有列
            for col in df.columns:
                val = row[col]
                if pd.notna(val):
                    record[col] = val
            records.append(record)
        return records

    def _process_imformation_pred_sheet_11(self, df, data_date, sheet_name, data_type):
        """自动生成的处理函数: 备用预测信息(2025-12-23) (模式: time_series_matrix)"""
        records = []
        df = df.dropna(how='all')
        df.columns = [str(c).strip() for c in df.columns]
        
        # 识别时间列
        time_cols = [c for c in df.columns if re.match(r'^\d{1,2}:\d{2}$', c)]
        
        for _, row in df.iterrows():
            # 假设 '数据项' 列是指标名称
            channel_name = str(row.get('数据项', 'Unknown')).strip()
            
            for t in time_cols:
                val = row[t]
                if pd.isna(val): continue
                
                records.append({
                    'record_date': data_date,
                    'record_time': t,
                    'channel_name': channel_name,
                    'value': val,
                    'sheet_name': sheet_name,
                    'type': data_type,
                    'created_at': datetime.datetime.now()
                })
        return records

    def _process_imformation_pred_sheet_12(self, df, data_date, sheet_name, data_type):
        """自动生成的处理函数: 阻塞预测信息(2025-12-23) (模式: generic_table)"""
        records = []
        df = df.dropna(how='all')
        df.columns = [str(c).strip() for c in df.columns]
        
        # 通用表格处理 (直接映射所有列)
        for _, row in df.iterrows():
            record = {
                'record_date': data_date,
                'sheet_name': sheet_name,
                'type': data_type,
                'created_at': datetime.datetime.now()
            }
            # 动态映射所有列
            for col in df.columns:
                val = row[col]
                if pd.notna(val):
                    record[col] = val
            records.append(record)
        return records

    def _process_imformation_pred_sheet_13(self, df, data_date, sheet_name, data_type):
        """自动生成的处理函数: 日前阻塞断面信息(2025-12-23) (模式: time_series_matrix)"""
        records = []
        df = df.dropna(how='all')
        df.columns = [str(c).strip() for c in df.columns]
        
        # 识别时间列
        time_cols = [c for c in df.columns if re.match(r'^\d{1,2}:\d{2}$', c)]
        
        for _, row in df.iterrows():
            # 假设 '断面名称' 列是指标名称
            channel_name = str(row.get('断面名称', 'Unknown')).strip()
            
            for t in time_cols:
                val = row[t]
                if pd.isna(val): continue
                
                records.append({
                    'record_date': data_date,
                    'record_time': t,
                    'channel_name': channel_name,
                    'value': val,
                    'sheet_name': sheet_name,
                    'type': data_type,
                    'created_at': datetime.datetime.now()
                })
        return records

    def _process_imformation_pred_sheet_14(self, df, data_date, sheet_name, data_type):
        """自动生成的处理函数: 必开必停机组（群）约束预测信息(2025-12-23) (模式: time_series_matrix)"""
        records = []
        df = df.dropna(how='all')
        df.columns = [str(c).strip() for c in df.columns]

        # 识别时间列
        time_cols = [c for c in df.columns if re.match(r'^\d{1,2}:\d{2}$', c)]

        for _, row in df.iterrows():
            channel_name = str(row.get('机组群名', 'Unknown')).strip()

            for t in time_cols:
                val = row[t]
                if pd.isna(val):
                    continue
                # 该表常见为文本约束值（如“必开/必停/自由优化”），为写入 power_data.value(DECIMAL) 做数值化。
                if isinstance(val, str):
                    s = val.strip()
                    if s in {"必开", "必须开机", "开机"}:
                        val = 1
                    elif s in {"必停", "必须停机", "停机"}:
                        val = -1
                    elif s in {"自由优化", "无约束", "正常"}:
                        val = 0
                    else:
                        # 兜底：尝试把字符串数值化
                        try:
                            val = float(s.replace(",", ""))
                        except Exception:
                            # 保留原值（若仍为字符串，将被 import_imformation_pred 分流到自定义表）
                            val = s

                records.append({
                    'record_date': data_date,
                    'record_time': t,
                    'channel_name': channel_name,
                    'value': val,
                    'sheet_name': sheet_name,
                    'type': data_type,
                    'created_at': datetime.datetime.now()
                })
        return records

    def _process_imformation_pred_sheet_15(self, df, data_date, sheet_name, data_type):
        """自动生成的处理函数: 必开必停机组信息预测信息(2025-12-23) (模式: time_series_matrix)"""
        records = []
        df = df.dropna(how='all')
        df.columns = [str(c).strip() for c in df.columns]

        # 识别时间列
        time_cols = [c for c in df.columns if re.match(r'^\d{1,2}:\d{2}$', c)]

        for _, row in df.iterrows():
            channel_name = (
                str(row.get('电厂名称', 'Unknown')).strip()
                + str(row.get('机组名称', 'Unknown')).strip()
                + str(row.get('数据类型', 'Unknown')).strip()
            )

            for t in time_cols:
                val = row[t]
                if pd.isna(val):
                    continue

                records.append({
                    'record_date': data_date,
                    'record_time': t,
                    'channel_name': channel_name,
                    'value': val,
                    'sheet_name': sheet_name,
                    'type': data_type,
                    'created_at': datetime.datetime.now()
                })
        return records

    def _process_imformation_pred_sheet_16(self, df, data_date, sheet_name, data_type):
        """自动生成的处理函数: 开停机不满足最小约束时间机组信息(2025-12-23) (模式: time_series_matrix)"""
        records = []
        df = df.dropna(how='all')
        df.columns = [str(c).strip() for c in df.columns]
        
        # 识别时间列
        time_cols = [c for c in df.columns if re.match(r'^\d{1,2}:\d{2}$', c)]
        
        for _, row in df.iterrows():
            # 假设 '机组名称' 列是指标名称
            channel_name = str(row.get('机组名称', 'Unknown')).strip()
            
            for t in time_cols:
                val = row[t]
                if val == "自由优化":
                    val = 1
                else:
                    val = 0
                if pd.isna(val): continue
                
                records.append({
                    'record_date': data_date,
                    'record_time': t,
                    'channel_name': channel_name,
                    'value': val,
                    'sheet_name': sheet_name,
                    'type': data_type,
                    'created_at': datetime.datetime.now()
                })
        return records

    def _process_imformation_pred_sheet_17(self, df, data_date, sheet_name, data_type):
        """自动生成的处理函数: 必开必停容量预测信息(2025-12-23) (模式: standard_list)"""
        records = []
        df = df.dropna(how='all')
        df.columns = [str(c).strip() for c in df.columns]
        
        # 标准列表处理
        for _, row in df.iterrows():
            # 解析日期（部分文件可能没有“日期”列）
            date_val = row.get('日期')
            r_date = pd.to_datetime(date_val).date() if pd.notna(date_val) else data_date
            channel_val = row.get('类型')
            channel = str(channel_val).strip() if pd.notna(channel_val) else "Unknown"
            
            # 遍历可能的数值列
            value_cols = ['序号', '必开机组容量(MW)', '必停机组容量(MW)']
            for col in value_cols:
                if col not in df.columns:
                    continue
                val = row.get(col)
                if pd.isna(val): continue
                
                # 如果有多列数值，将列名拼接到 channel_name
                final_channel = f'{channel}-{col}' if len(value_cols) > 1 else channel
                
                records.append({
                    'record_date': r_date,
                    'record_time': None,
                    'channel_name': final_channel,
                    'value': val,
                    'sheet_name': sheet_name,
                    'type': data_type,
                    'created_at': datetime.datetime.now()
                })
        return records

    def _process_imformation_pred_sheet_18(self, df, data_date, sheet_name, data_type):
        """自动生成的处理函数: 机组出力受限情况(2025-12-23) (模式: generic_table)"""
        records = []
        df = df.dropna(how='all')
        df.columns = [str(c).strip() for c in df.columns]
        
        # 通用表格处理 (直接映射所有列)
        for _, row in df.iterrows():
            record = {
                'record_date': data_date,
                'sheet_name': sheet_name,
                'type': data_type,
                'created_at': datetime.datetime.now()
            }
            # 动态映射所有列
            for col in df.columns:
                val = row[col]
                if pd.notna(val):
                    record[col] = val
            records.append(record)
        return records

    def _process_imformation_pred_sheet_19(self, df, data_date, sheet_name, data_type):
        """自动生成的处理函数: 储能机组指定模式清单(2025-12-23) (模式: generic_table)"""
        records = []
        df = df.dropna(how='all')
        df.columns = [str(c).strip() for c in df.columns]
        
        # 通用表格处理 (直接映射所有列)
        for _, row in df.iterrows():
            record = {
                'record_date': data_date,
                'sheet_name': sheet_name,
                'type': data_type,
                'created_at': datetime.datetime.now()
            }
            # 动态映射所有列
            for col in df.columns:
                val = row[col]
                if pd.notna(val):
                    record[col] = val
            records.append(record)
        return records

    def _process_imformation_pred_sheet_20(self, df, data_date, sheet_name, data_type):
        """自动生成的处理函数: 日前出清情况-机组详情（2025-12-23） (模式: time_series_matrix)"""
        records = []
        df = df.dropna(how='all')
        df.columns = [str(c).strip() for c in df.columns]
        
        # 识别时间列
        time_cols = [c for c in df.columns if re.match(r'^\d{1,2}:\d{2}$', c)]
        
        for _, row in df.iterrows():
            # 假设 '电厂名称' 列是指标名称
            channel_name = str(row.get('电厂名称', 'Unknown')).strip()
            
            for t in time_cols:
                val = row[t]
                if pd.isna(val): continue
                
                records.append({
                    'record_date': data_date,
                    'record_time': t,
                    'channel_name': channel_name,
                    'value': val,
                    'sheet_name': sheet_name,
                    'type': data_type,
                    'created_at': datetime.datetime.now()
                })
        return records

    def _process_imformation_pred_sheet_21(self, df, data_date, sheet_name, data_type):
        """自动生成的处理函数: 日前出清情况-节点详情（2025-12-23） (模式: time_series_matrix)"""
        records = []
        df = df.dropna(how='all')
        df.columns = [str(c).strip() for c in df.columns]
        
        # 识别时间列
        time_cols = [c for c in df.columns if re.match(r'^\d{1,2}:\d{2}$', c)]
        
        for _, row in df.iterrows():
            # 假设 '地区' 列是指标名称
            channel_name = str(row.get('地区', 'Unknown')).strip()
            
            for t in time_cols:
                val = row[t]
                if pd.isna(val): continue
                
                records.append({
                    'record_date': data_date,
                    'record_time': t,
                    'channel_name': channel_name,
                    'value': val,
                    'sheet_name': sheet_name,
                    'type': data_type,
                    'created_at': datetime.datetime.now()
                })
        return records
