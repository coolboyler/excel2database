import pandas as pd
import datetime
import re
from sqlalchemy import text
from database import DatabaseManager

class PowerDataImporter:
    def __init__(self):
        self.db_manager = DatabaseManager()

    # ===============================
    # 主入口：导入 Excel
    # ===============================
    def import_power_data(self, excel_file):
        """自动导入 Excel 中所有 Sheet，并统一处理"""
        try:
            sheet_dict = pd.read_excel(excel_file, sheet_name=None, header=0)
            print(f"✅ 成功读取 Excel，共 {len(sheet_dict)} 个 Sheet: {list(sheet_dict.keys())}")
        except Exception as e:
            print(f"❌ 读取 Excel 失败: {e}")
            return False

        # 自动识别文件类型（汉字）
        file_name = str(excel_file)
        chinese_match = re.search(r'([\u4e00-\u9fff]+)', file_name)
        data_type = chinese_match.group(1) if chinese_match else "未知类型"
        print(f"📁 文件类型识别: {data_type}")

        all_records = []

        for sheet_name, df in sheet_dict.items():
            print(f"\n🔹 正在处理 Sheet: {sheet_name}")

            # 识别 Sheet 日期
            match = re.search(r"\((\d{4}-\d{2}-\d{2})\)", sheet_name)
            data_date = match.group(1) if match else datetime.datetime.now().strftime('%Y-%m-%d')

            records = self._process_sheet(df, data_date, sheet_name, data_type)
            all_records.extend(records)
            print(f"✅ {sheet_name} 解析完成，共 {len(records)} 条记录")

        if not all_records:
            print("❌ 没有生成任何有效记录")
            return False

        return self.save_to_database(all_records, data_date)

    # ===============================
    # 核心 sheet 处理方法
    # ===============================
    def _process_sheet(self, df, data_date, sheet_name, data_type):
        """根据 Sheet 结构自动识别处理逻辑"""
        records = []

        df = df.dropna(how="all")  # 删除全空行
        if df.empty:
            print(f"⚠️ {sheet_name} 为空，跳过")
            return records

        # 清理列名
        df.columns = [str(c).strip() for c in df.columns]

        # 1️⃣ 如果是名单表（含 '电厂名称' 和 '机组名称'）
        if "电厂名称" in df.columns and "机组名称" in df.columns:
            records = self._process_unit_list(df, data_date, sheet_name, data_type)
            return records

        # 2️⃣ 如果有 '通道名称' 列
        if "通道名称" in df.columns:
            records = self._process_channel_format(df, data_date, sheet_name, data_type)
            return records

        # 3️⃣ 如果有 '类型' 列
        if "类型" in df.columns:
            # 判断是否是时刻列（00:00等）还是单值列
            time_cols = [c for c in df.columns if re.match(r"\d{1,2}:\d{2}", c)]
            if time_cols:
                records = self._process_type_format(df, data_date, sheet_name, data_type)
            else:
                records = self._process_type_date_value(df, data_date, sheet_name, data_type)
            return records

        # 4️⃣ 第一行是指标名 → 其他列是数值
        if not df.empty:
            records = self._process_first_row_as_channel(df, data_date, sheet_name, data_type)
            return records

        print(f"⚠️ 未识别 {sheet_name} 的处理规则")
        return records

    # ===============================
    # 处理单元/机组名单表
    # ===============================
    def _process_unit_list(self, df, data_date, sheet_name, data_type):
        """channel_name = 电厂名称-机组名称-类型，value 默认为 1"""
        records = []

        df = df.dropna(how="all")
        df.columns = [str(c).strip() for c in df.columns]

        for _, row in df.iterrows():
            record_date = data_date
            if "日期" in df.columns and pd.notna(row["日期"]):
                try:
                    record_date = pd.to_datetime(str(row["日期"])).date()
                except:
                    pass

            parts = []
            for col in ["电厂名称", "机组名称", "类型"]:
                if col in df.columns and pd.notna(row[col]):
                    parts.append(str(row[col]).strip())
            if not parts:
                continue

            records.append({
                "record_date": record_date,
                "record_time": datetime.datetime.now().time(),
                "channel_name": "-".join(parts),
                "value": 1,
                "type": data_type,
                "sheet_name": sheet_name,
                "created_at": datetime.datetime.now(),
            })

        return records

    # ===============================
    # 处理有通道名称的 Sheet
    # ===============================
    def _process_channel_format(self, df, data_date, sheet_name, data_type):
        records = []
        df = df.dropna(how="all")
        time_cols = [c for c in df.columns if re.match(r"\d{1,2}:\d{2}", c)]
        if not time_cols:
            return records

        for _, row in df.iterrows():
            channel_name = str(row["通道名称"]).strip()
            if not channel_name:
                continue
            for t in time_cols:
                value = row[t]
                if pd.isna(value):
                    continue
                try:
                    value = float(value)
                except:
                    continue
                records.append({
                    "record_date": pd.to_datetime(data_date).date(),
                    "record_time": t,
                    "channel_name": channel_name,
                    "value": value,
                    "type": data_type,
                    "sheet_name": sheet_name,
                    "created_at": datetime.datetime.now(),
                })
        return records

    # ===============================
    # 处理有类型列和时间列的 Sheet
    # ===============================
    def _process_type_format(self, df, data_date, sheet_name, data_type):
        records = []
        df = df.dropna(how="all")
        time_cols = [c for c in df.columns if re.match(r"\d{1,2}:\d{2}", c)]
        if not time_cols:
            return records

        for _, row in df.iterrows():
            parts = []
            if "类型" in df.columns and pd.notna(row["类型"]):
                parts.append(str(row["类型"]).strip())
            if "电源类型" in df.columns and pd.notna(row["电源类型"]):
                parts.append(str(row["电源类型"]).strip())
            if not parts:
                continue
            channel_name = "-".join(parts)

            for t in time_cols:
                value = row[t]
                if pd.isna(value):
                    continue
                try:
                    value = float(value)
                except:
                    continue
                records.append({
                    "record_date": pd.to_datetime(data_date).date(),
                    "record_time": t,
                    "channel_name": channel_name,
                    "value": value,
                    "type": data_type,
                    "sheet_name": sheet_name,
                    "created_at": datetime.datetime.now(),
                })
        return records

    # ===============================
    # 处理类型-日期-数值表
    # ===============================
    def _process_type_date_value(self, df, data_date, sheet_name, data_type):
        records = []
        df = df.dropna(how="all")
        df.columns = [str(c).strip() for c in df.columns]

        col_type = "类型" if "类型" in df.columns else None
        col_date = "日期" if "日期" in df.columns else None
        value_cols = [c for c in df.columns if c not in [col_type, col_date]]
        if not value_cols:
            return records
        value_col = value_cols[0]

        for _, row in df.iterrows():
            channel_name = str(row[col_type]).strip() if col_type else "未知类型"
            raw_date = str(row[col_date]).strip() if col_date and pd.notna(row[col_date]) else None

            parsed_date = None
            if raw_date:
                try:
                    parsed_date = pd.to_datetime(raw_date).date()
                except:
                    match = re.search(r"\((\d{2})\.(\d{2})", raw_date)
                    year_match = re.search(r"(\d{4})年", raw_date)
                    if match and year_match:
                        year = int(year_match.group(1))
                        month = int(match.group(1))
                        day = int(match.group(2))
                        parsed_date = datetime.date(year, month, day)
            if parsed_date is None:
                parsed_date = pd.to_datetime(data_date).date()

            try:
                value = float(row[value_col])
            except:
                continue

            records.append({
                "record_date": parsed_date,
                "record_time": datetime.datetime.now().time(),
                "channel_name": channel_name,
                "value": value,
                "type": data_type,
                "sheet_name": sheet_name,
                "created_at": datetime.datetime.now(),
            })
        return records

    # ===============================
    # 处理第一行是指标名的 Sheet
    # ===============================
    def _process_first_row_as_channel(self, df, data_date, sheet_name, data_type):
        records = []
        df = df.dropna(how="all").dropna(axis=1, how="all")
        if df.empty:
            return records

        channel_names = [str(c).strip() for c in df.iloc[0].tolist()]
        df = df.iloc[1:]
        if df.empty:
            return records

        for _, row in df.iterrows():
            for idx, value in enumerate(row):
                if idx >= len(channel_names):
                    continue
                if pd.isna(value):
                    continue
                records.append({
                    "record_date": pd.to_datetime(data_date).date(),
                    "record_time": datetime.datetime.now().time(),
                    "channel_name": channel_names[idx],
                    "value": value,
                    "type": data_type,
                    "sheet_name": sheet_name,
                    "created_at": datetime.datetime.now(),
                })
        return records

    # ===============================
    # 数据保存
    # ===============================
    def save_to_database(self, records, data_date):
        if not records:
            print("❌ 没有可保存的记录")
            return False

        if isinstance(records, pd.DataFrame):
            records = records.to_dict(orient="records")

        valid_records = []
        required_fields = ["record_date", "record_time", "channel_name", "value", "type", "sheet_name"]
        for i, r in enumerate(records):
            if not isinstance(r, dict):
                continue
            if not all(k in r for k in required_fields):
                continue
            if isinstance(r["record_date"], str):
                try:
                    r["record_date"] = pd.to_datetime(r["record_date"]).date()
                except:
                    continue
            valid_records.append(r)

        if not valid_records:
            print("❌ 没有可保存的有效记录")
            return False

        try:
            with self.db_manager.engine.begin() as conn:
                conn.execute(text("DELETE FROM power_data WHERE record_date = :record_date"), {"record_date": data_date})
                conn.execute(
                    text("""INSERT INTO power_data (record_date, record_time, channel_name, value, type, sheet_name, created_at)
                           VALUES (:record_date, :record_time, :channel_name, :value, :type, :sheet_name, :created_at)"""),
                    valid_records
                )
            print(f"✅ 数据已保存，共 {len(valid_records)} 条记录")
            return True
        except Exception as e:
            print(f"❌ 数据保存失败: {e}")
            return False
