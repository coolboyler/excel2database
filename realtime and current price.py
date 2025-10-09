import pandas as pd
import mysql.connector
from pandas._libs.tslibs.timestamps import Timestamp
from tqdm import tqdm


def process_and_insert_station_data(df, data_type, sheet_name, table_name):
    """
    对每个电站的数据进行处理并以批处理方式插入到指定数据库表的函数
    """
    grouped = df.groupby('节点名称')

    # 数据库连接配置
    db_config = {
        "user": "root",
        "password": "991006",
        "host": "localhost",
        "database": "electricity_db"
    }

    batch_size = 90000  # 批处理大小
    all_data_to_insert = []  # 存储所有待插入的数据

    # 初始化连接和游标
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        conn.autocommit = False  # 关闭自动提交

        # 使用 tqdm 为电站分组的循环添加进度条
        for station_name, group in tqdm(grouped, desc=f"处理并插入 {table_name} 的电站数据"):
            # 去除包含空值的行
            group = group.dropna()

            # 假设时间列名在数据中是列名类似于 00:00、00:15 这样的格式，这里先获取这些列名
            time_columns = [col for col in group.columns if ':' in col]

            # 将宽表数据转换为长表数据，其中时间列转换为 record_time 列，对应的值转换为 value 列
            melted_group = pd.melt(group, id_vars=['节点名称', '数据项'], value_vars=time_columns, var_name='record_time', value_name='value')
            melted_group["record_time"] = pd.to_datetime(melted_group["record_time"], format='%H:%M').dt.time

            # 添加 record_date 列
            melted_group["record_date"] = pd.to_datetime("2025-09-18").date()

            # 设置数据类型
            melted_group["type"] = data_type

            # 设置站点名称
            melted_group["channel_name"] = melted_group['节点名称']

            # 将 Timestamp 类型转换为字符串格式
            melted_group["created_at"] = Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')

            # 设置工作表名称
            melted_group["sheet_name"] = sheet_name

            # 处理 value 字段，保留两位小数
            melted_group['value'] = melted_group['value'].round(2)

            data = melted_group[[
                "record_date", "record_time", "type",
                "channel_name", "value", "created_at", "sheet_name"
            ]].values.tolist()

            all_data_to_insert.extend(data)

            while len(all_data_to_insert) >= batch_size:
                batch_data = all_data_to_insert[:batch_size]
                all_data_to_insert = all_data_to_insert[batch_size:]

                # 构造插入 SQL
                sql = f"""
                INSERT INTO {table_name} 
                (record_date, record_time, type, channel_name, value, created_at, sheet_name) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """

                cursor.executemany(sql, batch_data)

        # 处理剩余不足 batch_size 的数据
        if all_data_to_insert:
            sql = f"""
            INSERT INTO {table_name} 
            (record_date, record_time, type, channel_name, value, created_at, sheet_name) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(sql, all_data_to_insert)

        conn.commit()
        print(f"成功向 {table_name} 插入数据")

    except Exception as e:
        if conn:
            conn.rollback()  # 出错时回滚
        print(f"向 {table_name} 插入失败：{str(e)}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.autocommit = True  # 恢复自动提交
            conn.close()


# 读取日前节点电价 Excel 文件（替换为你的文件路径）
excel_path_1 = "日前节点电价查询(2025-09-18).xlsx"
sheet_name_1 = "日前节点电价查询(2025-09-18)"
df_1 = pd.read_excel(excel_path_1, sheet_name_1)
process_and_insert_station_data(df_1, "日前节点电价查询", sheet_name_1, "current_node_electricity_price")

# 读取实时节点电价 Excel 文件（替换为你的文件路径）
excel_path_2 = "实时节点电价查询(2025-09-18).xlsx"
sheet_name_2 = "实时节点电价查询(2025-09-18)"
df_2 = pd.read_excel(excel_path_2, sheet_name_2)
process_and_insert_station_data(df_2, "实时节点电价查询", sheet_name_2, "realtime_node_electricity_price")
