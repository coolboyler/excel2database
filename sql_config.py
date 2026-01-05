
# sql_config.py

# 表源定义
TABLE_SOURCE_POWER = "power_data"
TABLE_SOURCE_WEATHER = "calendar_weather"

# 字段映射配置
# source: 数据来源表 (power_data 或 calendar_weather)
# name: 指标名称
# where: SQL 查询条件 (仅用于 power_data)
# column: 数据库列名 (仅用于 calendar_weather)
# json_key: JSON 字段中的 Key (仅用于 calendar_weather, 如果数据在 json 列中)

SQL_RULES = {
    # ====================
    # 1. 天气与日期相关
    # ====================
    "date": {
        "name": "日期",
        "source": TABLE_SOURCE_WEATHER,
        "column": "date"
    },
    "day_type": {
        "name": "类型天",
        "source": TABLE_SOURCE_WEATHER,
        "column": "day_type_cn"
    },
    "week_day": {
        "name": "星期",
        "source": TABLE_SOURCE_WEATHER,
        "column": "day_type"  # 暂用 day_type 替代，或者需要计算字段
    },
    "temperature": {
        "name": "温度",
        "source": TABLE_SOURCE_WEATHER,
        "column": "weather_json",
        "json_key": "temps" # 这是一个数组
    },
    "weather": {
        "name": "天气",
        "source": TABLE_SOURCE_WEATHER,
        "column": "weather_json",
        "json_key": "weather_types"
    },
    # 以下字段在 calendar_weather 中没有直接列，可能在 json 中或者缺失
    "wind_direction": {
        "name": "风向",
        "source": TABLE_SOURCE_WEATHER,
        "column": "weather_json",
        "json_key": "wind_directions" 
    },
    "wind_speed": {
        "name": "风速",
        "source": TABLE_SOURCE_WEATHER,
        "column": "weather_json",
        "json_key": "wind_speeds" 
    },
    "rain_prob": {
        "name": "降雨概率",
        "source": TABLE_SOURCE_WEATHER,
        "column": "weather_json",
        "json_key": "rain_probs"
    },
    "apparent_temp": {
        "name": "体感温度",
        "source": TABLE_SOURCE_WEATHER,
        "column": "weather_json",
        "json_key": "apparent_temps"
    },
    "humidity": {
        "name": "湿度",
        "source": TABLE_SOURCE_WEATHER,
        "column": "weather_json",
        "json_key": "humidities"
    },
    "uv_index": {
        "name": "紫外线",
        "source": TABLE_SOURCE_WEATHER,
        "column": "weather_json",
        "json_key": "uv_indices"
    },
    "cloud_cover": {
        "name": "云量",
        "source": TABLE_SOURCE_WEATHER,
        "column": "weather_json",
        "json_key": "cloud_covers"
    },

    # ====================
    # 2. 价格指标
    # ====================
    "price_da": {
        "name": "日前节点",
        "source": TABLE_SOURCE_POWER,
        "where": "(channel_name LIKE '%均值%' OR channel_name = '节点电价') AND (type LIKE '%日前节点电价%' OR type LIKE '%日前%')"
    },
    "price_rt": {
        "name": "实时节点",
        "source": TABLE_SOURCE_POWER,
        "where": "(channel_name LIKE '%均值%' OR channel_name = '节点电价') AND (type LIKE '%实时节点电价%' OR type LIKE '%实时%')"
    },
    # 价差节点电价 (计算字段，代码处理)

    # ====================
    # 3. 预测负荷/电源
    # ====================
    "load_forecast": {
        "name": "统调预测",
        "source": TABLE_SOURCE_POWER,
        "where": "channel_name LIKE '%统调负荷%' AND type LIKE '%预测%'"
    },
    "class_a_forecast": {
        "name": "A类电源预测",
        "source": TABLE_SOURCE_POWER,
        "where": "channel_name LIKE '%A类%' AND type LIKE '%预测%'"
    },
    "class_b_forecast": {
        "name": "B类电源预测",
        "source": TABLE_SOURCE_POWER,
        "where": "channel_name LIKE '%B类%' AND type LIKE '%预测%'"
    },
    "local_power_forecast": {
        "name": "地方电源预测",
        "source": TABLE_SOURCE_POWER,
        "where": "channel_name LIKE '%地方%' AND type LIKE '%预测%'"
    },
    "west_east_forecast": {
        "name": "西电东送电源预测",
        "source": TABLE_SOURCE_POWER,
        "where": "channel_name LIKE '%西电%' AND type LIKE '%预测%'"
    },
    "guangdong_hongkong_forecast": {
        "name": "粤港澳预测", # 可能是粤港联络线
        "source": TABLE_SOURCE_POWER,
        "where": "channel_name LIKE '%粤港%' AND type LIKE '%预测%'"
    },
    "gen_total_forecast": {
        "name": "发电总预测",
        "source": TABLE_SOURCE_POWER,
        "where": "channel_name LIKE '%发电总%' AND type LIKE '%预测%'"
    },
    "spot_ne_d_forecast": {
        "name": "现货新能源D日预测",
        "source": TABLE_SOURCE_POWER,
        "where": "channel_name LIKE '%D日%' AND type LIKE '%预测%'"
    },
    "ne_pv_forecast": {
        "name": "统调新能源光伏预测",
        "source": TABLE_SOURCE_POWER,
        "where": "channel_name LIKE '%光伏%' AND type LIKE '%预测%'"
    },
    "ne_wind_forecast": {
        "name": "统调新能源风电预测",
        "source": TABLE_SOURCE_POWER,
        "where": "channel_name LIKE '%风电%' AND type LIKE '%预测%'"
    },
    "hydro_forecast": {
        "name": "水电含抽蓄预测",
        "source": TABLE_SOURCE_POWER,
        "where": "channel_name LIKE '%水电（含抽蓄）%' AND type LIKE '%预测%'"
    },

    # ====================
    # 4. 实际负荷/电源
    # ====================
    "load_actual": {
        "name": "实际统调负荷",
        "source": TABLE_SOURCE_POWER,
        "where": "channel_name LIKE '%统调%' AND type LIKE '%实际%'"
    },
    "class_a_actual": {
        "name": "A类电源实际",
        "source": TABLE_SOURCE_POWER,
        "where": "channel_name LIKE '%A类%' AND type LIKE '%实际%'"
    },
    "class_b_actual": {
        "name": "B类电源实际",
        "source": TABLE_SOURCE_POWER,
        "where": "channel_name LIKE '%B类%' AND type LIKE '%实际%'"
    },
    "local_power_actual": {
        "name": "地方电源实际",
        "source": TABLE_SOURCE_POWER,
        "where": "channel_name LIKE '%地方%' AND type LIKE '%实际%'"
    },
    "west_east_actual": {
        "name": "西电东送实际",
        "source": TABLE_SOURCE_POWER,
        "where": "channel_name LIKE '%西电东送%' AND type LIKE '%实际%'"
    },
    "guangdong_hongkong_actual": {
        "name": "粤港联络实际",
        "source": TABLE_SOURCE_POWER,
        "where": "channel_name LIKE '%粤港%' AND type LIKE '%实际%'"
    },
    "ne_total_actual": {
        "name": "新能源总实际",
        "source": TABLE_SOURCE_POWER,
        "where": "channel_name LIKE '%新能源%' AND type LIKE '%实际%'"
    },
    "hydro_actual": {
        "name": "水电含抽蓄实际",
        "source": TABLE_SOURCE_POWER,
        "where": "channel_name LIKE '%水电（含抽蓄）%' AND type LIKE '%实际%'"
    },

    # ====================
    # 5. 偏差
    # ====================

    "load_deviation": {
        "name": "统调负荷偏差",
        "source": TABLE_SOURCE_POWER,
        "where": "channel_name LIKE '%偏差%' OR channel_name LIKE '%统调负荷偏差%'" # 待确认
    }
}
