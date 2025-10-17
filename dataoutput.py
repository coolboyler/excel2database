import pandas as pd
import mysql.connector
import numpy as np

# 数据库连接配置
db_config = {
    "user": "root",
    "password": "991006",
    "host": "localhost",
    "database": "electricity_db"
}

# 查询语句
query = """
SELECT 
  channel_name, 
  record_date, 
  sheet_name,
  HOUR(record_time) AS hour,
  ROUND(AVG(value), 2) AS avg_value 
FROM 
  electricity_db.realtime_node_electricity_price 
GROUP BY 
  channel_name, record_date, sheet_name, HOUR(record_time)
"""

try:
    conn = mysql.connector.connect(**db_config)
    df = pd.read_sql(query, conn)

    # --------------------------
    # 核心：结合sheet_name和日期动态生成文件名
    # --------------------------
    # 提取唯一的sheet_name（假设数据中sheet_name唯一）
    sheet_name = df['sheet_name'].unique()[0]
    # 提取唯一的日期（假设数据中日期唯一）
    record_date = df['record_date'].unique()[0].strftime('%Y-%m-%d')  # 格式化日期为YYYY-MM-DD
    # 处理文件名特殊字符（避免斜杠、空格等导致保存失败）
    sheet_name_clean = sheet_name.replace('/', '_').replace('\\', '_').replace(' ', '')
    # 构造文件名：{sheet_name}({日期})_小时.xlsx
    output_path = f"{sheet_name_clean}({record_date})_小时.xlsx"


    # 生成电站级透视表
    pivot_df = pd.pivot_table(
        df,
        index=['channel_name', 'record_date'],
        columns='hour',
        values='avg_value',
        aggfunc='mean'
    )
    pivot_df = pivot_df.reindex(columns=range(24), fill_value=np.nan)
    pivot_df.columns = [f'{h}:00' for h in pivot_df.columns]
    pivot_df = pivot_df.reset_index()


    # 修改前两列名称
    pivot_df = pivot_df.rename(columns={
        'channel_name': '节点名称',
        'record_date': '日期'
    })


    # 插入单位列
    pivot_df.insert(
        loc=2,
        column='单位',
        value='电价(元/MWh)'
    )


    # 添加发电侧全省统一均价行
    hour_columns = [f'{h}:00' for h in range(24)]
    province_avg = pivot_df[hour_columns].mean(skipna=True).round(2)

    province_row = pd.DataFrame({
        '节点名称': ['发电侧全省统一均价'],
        '日期': [record_date],
        '单位': ['电价(元/MWh)'],**{col: [province_avg[col]] for col in hour_columns}
    })

    final_df = pd.concat([pivot_df, province_row], ignore_index=True)


    # 导出为Excel（动态文件名）
    final_df.to_excel(output_path, index=False, engine='openpyxl')
    print(f"数据已成功导出到 {output_path}")

except Exception as e:
    print(f"导出失败：{str(e)}")

finally:
    if conn:
        conn.close()
