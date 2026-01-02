import pandas as pd
import re
import datetime

class AutoImporterGenerator:
    def __init__(self):
        # 英文翻译映射字典 (简单示例，可扩展)
        self.translation_map = {
            "电厂名称": "power_plant_name",
            "机组名称": "generator_name",
            "最小技术出力": "min_technical_output",
            "最小技术出力(MW)": "min_technical_output",
            "额定出力": "rated_output",
            "额定出力(MW)": "rated_output",
            "日期": "maintenance_date", # 修正：日期通常是检修日期
            "时间": "record_time",
            "类型": "type",
            "备注": "remarks",
            "序号": "seq_no",
            "元件名称": "component_name", # 添加
            "设备名称": "device_name",
            "电压等级": "voltage_level",
            "电压等级(Kv)": "voltage_level", # 添加
            "停电范围": "outage_scope",
            "停电时间": "outage_time",
            "送电时间": "restore_time",
            "工作内容": "work_content",
            "检修性质": "maintenance_type",
            "申请单位": "applicant",
            # Sheet名翻译
            "机组检修预测信息": "unit_maintenance_prediction",
            "机组技术参数": "unit_technical_parameters",
            "检修计划": "maintenance_plan",
            "输变电检修预测信息": "transmission_maintenance", # 添加
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
            # ... 添加更多映射
        }
        
    def translate_col(self, col_name):
        """尝试翻译列名，如果找不到则拼音或保留"""
        # 1. 查字典
        clean_col = str(col_name).strip()
        if clean_col in self.translation_map:
            return self.translation_map[clean_col]
        
        # 2. 尝试部分匹配 (例如 "最小技术出力(MW)" -> "min_technical_output")
        for k, v in self.translation_map.items():
            if k in clean_col:
                return v
        
        # 3. 如果是纯中文，简单转拼音? 这里暂时用 safe_name
        # 实际生产环境可以使用 pypinyin 库
        # 这里仅做 safe 处理
        return clean_col.replace("(", "_").replace(")", "_").replace(" ", "_").strip()

    def analyze_and_generate(self, file_path):
        """
        分析Excel文件并生成Importer代码
        """
        try:
            xls = pd.ExcelFile(file_path)
            sheet_names = xls.sheet_names
        except Exception as e:
            return {"error": str(e)}

        # 清理文件名中的日期部分，生成通用的函数名
        # 例如: "2023-01-01_Test" -> "Test"
        base_name = file_path.split("/")[-1].replace(".xlsx", "")
        # 移除日期模式 YYYY-MM-DD 或 YYYYMMDD
        base_name_clean = re.sub(r'\d{4}[-_]?\d{1,2}[-_]?\d{1,2}', '', base_name)
        # 移除可能剩下的首尾分隔符
        base_name_clean = re.sub(r'^[-_]+|[-_]+$', '', base_name_clean)
        
        # 如果清理后为空（例如文件名就是日期），则使用 Generic
        if not base_name_clean.strip():
            base_name_clean = "Generic"
            
        filename_clean = self.clean_name(base_name_clean)
        
        analysis_result = {
            "filename": file_path.split("/")[-1],
            "sheets": [],
            "generated_code": ""
        }

        code_buffer = []
        
        # 1. API Usage Snippet
        code_buffer.append(f"# ==========================================")
        code_buffer.append(f"# 1. 将以下代码添加到 api.py 的 import_file 函数中")
        code_buffer.append(f"# ==========================================")
        code_buffer.append(f"    # elif '{filename_clean}' in filename:")
        code_buffer.append(f"    #     method = importer.import_{filename_clean}")
        code_buffer.append(f"")
        code_buffer.append(f"# ==========================================")
        code_buffer.append(f"# 2. 将以下代码添加到 pred_reader.py 的 PowerDataImporter 类中")
        code_buffer.append(f"# ==========================================")
        code_buffer.append(f"")
        
        # 2. Main Import Method
        code_buffer.append(f"    def import_{filename_clean}(self, excel_file):")
        code_buffer.append(f"        \"\"\"自动生成的导入函数: {analysis_result['filename']} (类)\"\"\"")
        code_buffer.append(f"        try:")
        code_buffer.append(f"            sheet_dict = pd.read_excel(excel_file, sheet_name=None, header=0)")
        code_buffer.append(f"        except Exception as e:")
        code_buffer.append(f"            print(f\"❌ 无法读取Excel: {{e}}\")")
        code_buffer.append(f"            return False, None, 0, []")
        code_buffer.append(f"")
        code_buffer.append(f"        all_records = []")
        code_buffer.append(f"        data_date = None")
        code_buffer.append(f"        ")
        code_buffer.append(f"        # 尝试从文件名提取日期")
        code_buffer.append(f"        match = re.search(r'(\d{{4}}-\d{{1,2}}-\d{{1,2}})', str(excel_file))")
        code_buffer.append(f"        if match:")
        code_buffer.append(f"            data_date = datetime.datetime.strptime(match.group(1), '%Y-%m-%d').date()")
        code_buffer.append(f"        else:")
        code_buffer.append(f"            # 如果文件名没日期，尝试用当天或抛出警告")
        code_buffer.append(f"            print(f\"⚠️ 未能在文件名中识别日期，默认使用今日\")")
        code_buffer.append(f"            data_date = datetime.date.today()")
        code_buffer.append(f"")
        code_buffer.append(f"        # 根据文件名识别类型")
        code_buffer.append(f"        file_name = str(excel_file)")
        code_buffer.append(f"        chinese_match = re.search(r'([\u4e00-\u9fff]+)', file_name)")
        code_buffer.append(f"        if chinese_match:")
        code_buffer.append(f"            data_type = chinese_match.group(1)")
        code_buffer.append(f"            print(f\"📁 文件类型识别: {{data_type}}\")")
        code_buffer.append(f"        else:")
        code_buffer.append(f"            data_type = \"自动导入\"")
        code_buffer.append(f"            print(f\"⚠️ 未能在文件名中找到汉字，默认类型: {{data_type}}\")")
        code_buffer.append(f"")
        code_buffer.append(f"        sheet_names = list(sheet_dict.keys())")
        code_buffer.append(f"")

        for i, sheet_name in enumerate(sheet_names):
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=0)
            
            # 分析Sheet结构
            sheet_info = self.analyze_sheet(df, sheet_name)
            analysis_result["sheets"].append(sheet_info)

            # 生成对应的处理函数调用 - 使用索引而非名称
            func_name = f"_process_{filename_clean}_sheet_{i+1}"
            code_buffer.append(f"        # 处理第 {i+1} 个 Sheet (原名: {sheet_name})")
            code_buffer.append(f"        if len(sheet_names) > {i}:")
            code_buffer.append(f"            current_sheet_name = sheet_names[{i}]")
            code_buffer.append(f"            records = self.{func_name}(sheet_dict[current_sheet_name], data_date, current_sheet_name, data_type)")
            code_buffer.append(f"            all_records.extend(records)")
            code_buffer.append(f"")
        
        code_buffer.append(f"        if not all_records:")

        code_buffer.append(f"            print(\"❌ 没有生成任何有效记录\")")
        code_buffer.append(f"            return False, None, 0, []")
        code_buffer.append(f"")
        code_buffer.append(f"        # 保存到数据库")
        
        # 检查是否有 generic_table
        has_generic = any(s["pattern_type"] == "generic_table" for s in analysis_result["sheets"])
        if has_generic:
            code_buffer.append(f"        # 使用自定义保存方法 (包含Generic Table)")
            code_buffer.append(f"        return self.save_to_{filename_clean}_database(all_records, data_date)")
        else:
            code_buffer.append(f"        # 使用通用保存方法")
            code_buffer.append(f"        return self.save_to_database(all_records, data_date)")
            
        code_buffer.append(f"")

        # 3. Helper Methods
        for i, sheet_info in enumerate(analysis_result["sheets"]):
            sheet_name = sheet_info["name"]
            func_name = f"_process_{filename_clean}_sheet_{i+1}"
            func_code = self.generate_func_code(func_name, sheet_info)
            code_buffer.append(func_code)
            code_buffer.append("")
            
        # 4. Generate Custom Save Method (Optional)
        # 只有当包含 generic_table 时才生成自定义保存方法
        if any(s["pattern_type"] == "generic_table" for s in analysis_result["sheets"]):
            save_method_code = self.generate_custom_save_method(filename_clean, analysis_result["sheets"])
            code_buffer.append(save_method_code)
            code_buffer.append("")

        analysis_result["generated_code"] = "\n".join(code_buffer)
        return analysis_result

    def generate_custom_save_method(self, filename_clean, sheets):
        """
        生成自定义保存方法 (save_to_..._database)
        针对 Generic Table 生成特定的 CREATE TABLE 和 INSERT 语句
        """
        lines = []
        # 使用更简洁的表名，不包含 custom_ 前缀
        table_name = f"{filename_clean.lower()}"
        
        lines.append(f"    def save_to_{filename_clean}_database(self, records, data_date):")
        lines.append(f"        \"\"\"保存 {filename_clean} 数据到自定义表 {table_name}\"\"\"")
        lines.append(f"        if not records:")
        lines.append(f"            print(\"❌ 没有可保存的记录\")")
        lines.append(f"            return True, None, 0, []")
        lines.append(f"")
        lines.append(f"        # 1. 过滤无效记录")
        lines.append(f"        valid_records = []")
        lines.append(f"        for r in records:")
        lines.append(f"            if isinstance(r, dict):")
        lines.append(f"                r['record_date'] = data_date")
        lines.append(f"                valid_records.append(r)")
        lines.append(f"")
        lines.append(f"        if not valid_records:")
        lines.append(f"            return False, None, 0, []")
        lines.append(f"")
        
        # 针对每个 sheet 应该有独立的表结构设计，但如果都用同一个 save 方法，
        # 我们需要判断记录属于哪个 sheet，或者创建一个超级宽表。
        # 用户要求 "针对不能识别的sheet都要各自做对应的表结构设计"
        # 这意味着我们需要为每个 sheet 生成一个独立的表。
        # 但是，我们只有一个 save 方法入口。
        # 解决方案：在 save 方法内部，根据 sheet_name 分发到不同的表。
        
        lines.append(f"        # 分发到不同的表 (根据 sheet_name)")
        lines.append(f"        # 自动生成的表名映射")
        
        # 收集 generic sheets
        generic_sheets = [s for s in sheets if s["pattern_type"] == "generic_table"]
        
        lines.append(f"        try:")
        lines.append(f"            with self.db_manager.engine.begin() as conn:")
        
        for i, sheet in enumerate(generic_sheets):
            sheet_safe_name = self.clean_name(sheet["name"]).lower()
            # 基础Sheet名 (去除日期)，用于匹配和表名生成
            base_sheet_name = self.remove_date_from_sheetname(sheet["name"])
            
            # 尝试翻译 Base Sheet Name
            translated_base_name = self.translate_col(base_sheet_name)
            # 如果翻译失败（返回了拼音或原始值），且原始值包含中文，则这里可能还是有问题
            # 但 translate_col 目前只是查表，我们需要扩展它支持更多或者让用户去填
            # 这里我们使用 translated_base_name 作为表名的一部分
            base_sheet_safe = self.clean_name(translated_base_name).lower()
            
            # 表名规则: 文件名_base_sheet名
            current_table_name = f"{filename_clean.lower()}_{base_sheet_safe}"
            if filename_clean.lower() == base_sheet_safe or not base_sheet_safe:
                current_table_name = f"{filename_clean.lower()}_data"
                
            lines.append(f"                # --- 处理 Sheet: {sheet['name']} (Base: {base_sheet_name}) -> 表: {current_table_name} ---")
            # 使用更宽容的匹配逻辑: 只要 base_sheet_name 在 sheet_name 中即可 (且不包含日期干扰)
            # 或者，如果 base_sheet_name 很短，可能会误判。
            # 更安全的做法: 假设 import 阶段传进来的 sheet_name 是完整的。
            # 我们在代码里也做同样的 clean 操作来比较?
            # 为了简单有效，我们生成一行代码来检查:
            lines.append(f"                current_sheet_records = []")
            lines.append(f"                for r in valid_records:")
            lines.append(f"                    r_sheet = str(r.get('sheet_name', ''))")
            lines.append(f"                    # 移除日期后比较")
            lines.append(f"                    r_base = re.sub(r'\\d{{4}}[-/]?\\d{{1,2}}[-/]?\\d{{1,2}}', '', r_sheet).replace('()', '').strip()")
            lines.append(f"                    # 如果 base_name 包含在处理后的 r_base 中，或者 r_base 包含 base_name")
            lines.append(f"                    if '{base_sheet_name}' in r_base or r_base == '{base_sheet_name}':")
            lines.append(f"                        current_sheet_records.append(r)")
            
            lines.append(f"                if current_sheet_records:")
            lines.append(f"                    # 2. 创建表")
            lines.append(f"                    create_sql = f\"\"\"")
            lines.append(f"                    CREATE TABLE IF NOT EXISTS `{current_table_name}` (")
            lines.append(f"                        `id` bigint(20) NOT NULL AUTO_INCREMENT,")
            lines.append(f"                        `record_date` date DEFAULT NULL,")
            lines.append(f"                        `sheet_name` varchar(255) DEFAULT NULL,")
            lines.append(f"                        `type` varchar(100) DEFAULT NULL,")
            
            # 生成该 sheet 特有的列
            for col in sorted(sheet["columns"]):
                safe_col = self.translate_col(col)
                lines.append(f"                        `{safe_col}` text COMMENT '{col}',")
                
            lines.append(f"                        `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,")
            lines.append(f"                        PRIMARY KEY (`id`),")
            lines.append(f"                        KEY `idx_record_date` (`record_date`)")
            lines.append(f"                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")
            lines.append(f"                    \"\"\"")
            lines.append(f"                    conn.execute(text(create_sql))")
            lines.append(f"")
            lines.append(f"                    # 3. 删除旧数据")
            lines.append(f"                    conn.execute(text(f\"DELETE FROM {current_table_name} WHERE record_date = :date\"), {{'date': data_date}})")
            lines.append(f"")
            lines.append(f"                    # 4. 插入新数据")
            
            # 生成 INSERT 语句
            col_map = {}
            for col in sorted(sheet["columns"]):
                safe = self.translate_col(col)
                col_map[col] = safe
            
            # 关键修复：构建清洗后的记录用于插入 (修复绑定参数包含特殊字符的问题)
            lines.append(f"                    sanitized_records = []")
            lines.append(f"                    for r in current_sheet_records:")
            lines.append(f"                        new_r = {{'record_date': r['record_date'], 'sheet_name': r['sheet_name'], 'type': r['type']}}")
            # 动态映射：col_map 包含了 {原始列名: 安全列名}
            lines.append(f"                        for original_col, safe_col in {col_map}.items():")
            lines.append(f"                            if original_col in r:")
            lines.append(f"                                new_r[safe_col] = r[original_col]")
            lines.append(f"                        sanitized_records.append(new_r)")
            
            insert_cols = ['record_date', 'sheet_name', 'type']
            insert_params = [':record_date', ':sheet_name', ':type']
            
            for col in col_map:
                safe_name = col_map[col]
                insert_cols.append(f'`{safe_name}`')
                insert_params.append(f':{safe_name}') # 使用安全名作为参数名
            
            lines.append(f"                    insert_sql = text(f\"INSERT INTO {current_table_name} ({', '.join(insert_cols)}) VALUES ({', '.join(insert_params)})\")")
            lines.append(f"                    conn.execute(insert_sql, sanitized_records)")
            lines.append(f"                    print(f\"✅ 已保存 {{len(current_sheet_records)}} 条记录到 {current_table_name}\")")
            
            # 5. 获取预览数据 (针对第一个 sheet 或合并预览)
            # 这里简单取前 10 条
            lines.append(f"                    if i == 0: # 仅预览第一个匹配表的")
            lines.append(f"                        preview_data = sanitized_records[:10]")
            lines.append(f"")

        lines.append(f"                # 返回结果，包括预览数据")
        lines.append(f"                return True, \"{table_name}_*\", len(valid_records), preview_data if 'preview_data' in locals() else []")
        lines.append(f"        except Exception as e:")
        lines.append(f"            print(f\"❌ 保存失败: {{e}}\")")
        lines.append(f"            return False, None, 0, []")
        
        return "\n".join(lines)

    def analyze_sheet(self, df, sheet_name):
        """
        分析单个Sheet的结构
        """
        # 标准化列名
        df.columns = [str(c).strip() for c in df.columns]
        columns = df.columns.tolist()
        
        # 预览数据 (前3行，转dict)
        preview = df.head(3).where(pd.notnull(df), None).to_dict(orient='records')

        # 1. 检测是否包含时间列 (00:00 - 23:45)
        time_cols = [c for c in columns if re.match(r"^\d{1,2}:\d{2}$", c) or re.match(r"^\d{1,2}:\d{2}:\d{2}$", c)]
        
        pattern_type = "unknown"
        
        if len(time_cols) > 5:
            pattern_type = "time_series_matrix"  # 矩阵式：列是时间，行是指标
        elif "日期" in columns and ("类型" in columns or "通道名称" in columns):
             pattern_type = "standard_list" # 标准列表：有日期、类型、值
        else:
            pattern_type = "generic_table" # 通用表格

        return {
            "name": sheet_name,
            "rows": len(df),
            "cols": len(columns),
            "columns": columns,
            "pattern_type": pattern_type,
            "time_cols": time_cols,
            "preview": preview
        }

    def generate_func_code(self, func_name, sheet_info):
        """
        根据分析结果生成函数代码
        """
        pattern_type = sheet_info["pattern_type"]
        columns = sheet_info["columns"]
        
        lines = []
        lines.append(f"    def {func_name}(self, df, data_date, sheet_name, data_type):")
        lines.append(f"        \"\"\"自动生成的处理函数: {sheet_info['name']} (模式: {pattern_type})\"\"\"")
        lines.append(f"        records = []")
        lines.append(f"        df = df.dropna(how='all')")
        lines.append(f"        df.columns = [str(c).strip() for c in df.columns]")
        lines.append(f"        ")

        if pattern_type == "time_series_matrix":
            # 生成矩阵式处理代码
            lines.append(f"        # 识别时间列")
            lines.append(f"        time_cols = [c for c in df.columns if re.match(r'^\\d{{1,2}}:\\d{{2}}$', c)]")
            lines.append(f"        ")
            lines.append(f"        for _, row in df.iterrows():")
            
            # 猜测 channel_name 列
            candidate_name_cols = [c for c in columns if c not in sheet_info["time_cols"] and "日期" not in c]
            name_col = candidate_name_cols[0] if candidate_name_cols else "Unknown"
            
            lines.append(f"            # 假设 '{name_col}' 列是指标名称")
            lines.append(f"            channel_name = str(row.get('{name_col}', 'Unknown')).strip()")
            lines.append(f"            ")
            lines.append(f"            for t in time_cols:")
            lines.append(f"                val = row[t]")
            lines.append(f"                if pd.isna(val): continue")
            lines.append(f"                ")
            lines.append(f"                records.append({{")
            lines.append(f"                    'record_date': data_date,")
            lines.append(f"                    'record_time': t,")
            lines.append(f"                    'channel_name': channel_name,")
            lines.append(f"                    'value': val,")
            lines.append(f"                    'sheet_name': sheet_name,")
            lines.append(f"                    'type': data_type,")
            lines.append(f"                    'created_at': datetime.datetime.now()")
            lines.append(f"                }})")

        elif pattern_type == "standard_list":
            # 生成标准列表处理代码
            lines.append(f"        # 标准列表处理")
            lines.append(f"        for _, row in df.iterrows():")
            
            # 智能映射
            col_date = "日期" if "日期" in columns else None
            col_type = "类型" if "类型" in columns else ("通道名称" if "通道名称" in columns else None)
            
            # 寻找数值列 (排除日期和类型)
            value_cols = [c for c in columns if c not in [col_date, col_type]]
            
            if col_date:
                lines.append(f"            # 解析日期")
                lines.append(f"            r_date = pd.to_datetime(row['{col_date}']).date() if pd.notna(row['{col_date}']) else data_date")
            else:
                lines.append(f"            r_date = data_date")

            if col_type:
                 lines.append(f"            channel = str(row['{col_type}']).strip()")
            else:
                 lines.append(f"            channel = 'Default'")

            # 遍历剩余列作为值
            lines.append(f"            ")
            lines.append(f"            # 遍历可能的数值列")
            lines.append(f"            value_cols = {value_cols}")
            lines.append(f"            for col in value_cols:")
            lines.append(f"                val = row[col]")
            lines.append(f"                if pd.isna(val): continue")
            lines.append(f"                ")
            lines.append(f"                # 如果有多列数值，将列名拼接到 channel_name")
            lines.append(f"                final_channel = f'{{channel}}-{{col}}' if len(value_cols) > 1 else channel")
            lines.append(f"                ")
            lines.append(f"                records.append({{")
            lines.append(f"                    'record_date': r_date,")
            lines.append(f"                    'record_time': None,")
            lines.append(f"                    'channel_name': final_channel,")
            lines.append(f"                    'value': val,")
            lines.append(f"                    'sheet_name': sheet_name,")
            lines.append(f"                    'type': data_type,")
            lines.append(f"                    'created_at': datetime.datetime.now()")
            lines.append(f"                }})")

        else:
            # 通用表格处理 (映射所有列)
            lines.append(f"        # 通用表格处理 (直接映射所有列)")
            lines.append(f"        for _, row in df.iterrows():")
            lines.append(f"            record = {{")
            lines.append(f"                'record_date': data_date,")
            lines.append(f"                'sheet_name': sheet_name,")
            lines.append(f"                'type': data_type,")
            lines.append(f"                'created_at': datetime.datetime.now()")
            lines.append(f"            }}")
            lines.append(f"            # 动态映射所有列")
            lines.append(f"            for col in df.columns:")
            lines.append(f"                val = row[col]")
            lines.append(f"                if pd.notna(val):")
            lines.append(f"                    record[col] = val")
            lines.append(f"            records.append(record)")

        lines.append(f"        return records")
        return "\n".join(lines)

    def clean_name(self, name):
        """清理名称用于函数名"""
        # 移除非字母数字字符
        cleaned = re.sub(r'[^a-zA-Z0-9_]', '_', str(name))
        # 移除重复的下划线
        cleaned = re.sub(r'_+', '_', cleaned)
        # 移除首尾下划线
        return cleaned.strip('_')

    def remove_date_from_sheetname(self, sheet_name):
        """移除Sheet名中的日期 (例如 'Info(2025-12-23)' -> 'Info')"""
        # 移除 (YYYY-MM-DD) 或 (YYYY/MM/DD) 或 (YYYYMMDD)
        s = re.sub(r'\(\d{4}[-/]?\d{1,2}[-/]?\d{1,2}\)', '', str(sheet_name))
        # 移除 YYYY-MM-DD (无括号)
        s = re.sub(r'\d{4}[-/]?\d{1,2}[-/]?\d{1,2}', '', s)
        return s.strip()
