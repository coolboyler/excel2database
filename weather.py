import requests
import pandas as pd
import numpy as np
import datetime
import time

# 配置信息
GFS_API_URL = "https://api-pro-openet.terraqt.com/v1/gfs_surface/point"
OPENMETEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
OPENMETEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
API_TOKEN = "jBDMwYmN1ImNwADMwEGMwEWN3E2M1UTN"
LOCATION = {
    'lon': 113.37,
    'lat': 23.1
}

# --- 辅助函数 ---

def deg_to_cn_wind_dir(deg):
    if pd.isna(deg): return "风向不定"
    dirs = ['北风', '东北风', '东风', '东南风', '南风', '西南风', '西风', '西北风']
    idx = int((deg + 22.5) // 45) % 8
    return dirs[idx]

def get_weather_type_row(row):
    # 适配不同的源列名
    t2m = row.get('t2m') if 't2m' in row else row.get('temperature_2m', 0)
    d2m = row.get('d2m') if 'd2m' in row else row.get('dew_point_2m', t2m)
    tcc = row.get('tcc') if 'tcc' in row else row.get('cloud_cover', 0)
    tp = row.get('tp') if 'tp' in row else row.get('precipitation', 0)

    # 判断时间
    hour = row.name.hour if hasattr(row, 'name') and hasattr(row.name, "hour") else 12
    is_daytime = 5 <= hour < 20

    if tp >= 50: return "特大暴雨"
    elif tp > 30: return "暴雨"
    elif tp > 16: return "大到暴雨"
    elif tp > 10: return "大雨"
    elif tp > 2: return "中雨"
    elif tp > 0.5:
        if tcc > 85: return "阴天有小雨"
        elif tcc > 60: return "多云有小雨"
        else: return "局部阵雨"
    elif 0.1 < tp <= 0.5:
        if tcc > 60: return "毛毛雨"
        else: return "零星小雨"

    if tcc > 90: return "阴天"
    elif tcc > 70: return "多云" if is_daytime else "夜间多云"
    elif is_daytime:
        if (t2m - d2m) > 3: return "晴天"
        else: return "晴或少云"
    else:
        return "夜间晴"

def rain_probability_row(row):
    t2m = row.get('t2m') if 't2m' in row else row.get('temperature_2m', 0)
    d2m = row.get('d2m') if 'd2m' in row else row.get('dew_point_2m', t2m)
    tcc = row.get('tcc') if 'tcc' in row else row.get('cloud_cover', 0)
    tp = row.get('tp') if 'tp' in row else row.get('precipitation', 0)
    
    # dswrf (W/m2) -> uvb 近似值
    dswrf = row.get('dswrf') if 'dswrf' in row else row.get('shortwave_radiation', 0)
    uvb = dswrf / 10.0

    dew_gap = np.clip(t2m - d2m, 0, 10)
    rain_intensity = np.clip(tp * 10, 0, 40)
    cloud_factor = np.clip(tcc / 1.5, 0, 40)
    uv_factor = np.clip((40 - uvb) / 2, 0, 20)

    prob = rain_intensity + cloud_factor + uv_factor - dew_gap * 2
    return int(np.clip(prob, 0, 100))

# --- API 获取函数 ---

def process_openmeteo_data(data):
    if 'hourly' not in data: return None
    hourly = data['hourly']
    df = pd.DataFrame(hourly)
    df.index = pd.to_datetime(df['time'])
    
    df["wind_dir"] = df["wind_direction_10m"].apply(deg_to_cn_wind_dir)
    df["weather_type"] = df.apply(get_weather_type_row, axis=1)
    df["rain_prob"] = df.apply(rain_probability_row, axis=1)
    
    # 填充缺失值
    pd.set_option('future.no_silent_downcasting', True)
    df = df.fillna(0).infer_objects(copy=False)

    summary = {
        "max_temp": float(df["temperature_2m"].max()),
        "min_temp": float(df["temperature_2m"].min()),
        "avg_temp": float(df["temperature_2m"].mean()),
        "weather_type": df["weather_type"].mode()[0] if not df["weather_type"].empty else "未知",
        "max_rain_prob": int(df["rain_prob"].max()),
        "temps": df["temperature_2m"].tolist(),
        "weather_types": df["weather_type"].tolist(),
        "rain_probs": df["rain_prob"].tolist(),
        "humidities": df["relative_humidity_2m"].tolist(),
        "apparent_temps": df["apparent_temperature"].tolist(),
        "wind_speeds": df["wind_speed_10m"].tolist(),
        "wind_directions": df["wind_dir"].tolist(),
        "cloud_covers": df["cloud_cover"].tolist(),
        "uv_indices": df.get("uv_index", [0]*24).tolist(),
        "source": "OpenMeteo"
    }
    return summary

def fetch_weather_openmeteo_archive(target_date):
    date_str = target_date.strftime("%Y-%m-%d")
    params = {
        'latitude': LOCATION['lat'],
        'longitude': LOCATION['lon'],
        'start_date': date_str,
        'end_date': date_str,
        'hourly': 'temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,cloud_cover,wind_speed_10m,wind_direction_10m,shortwave_radiation,uv_index',
        'timezone': 'Asia/Shanghai'
    }
    try:
        response = requests.get(OPENMETEO_ARCHIVE_URL, params=params, timeout=10)
        if response.status_code != 200: return None
        return process_openmeteo_data(response.json())
    except Exception: return None

def fetch_weather_openmeteo_forecast(target_date):
    date_str = target_date.strftime("%Y-%m-%d")
    params = {
        'latitude': LOCATION['lat'],
        'longitude': LOCATION['lon'],
        'start_date': date_str,
        'end_date': date_str,
        'hourly': 'temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,cloud_cover,wind_speed_10m,wind_direction_10m,shortwave_radiation,uv_index',
        'timezone': 'Asia/Shanghai'
    }
    try:
        response = requests.get(OPENMETEO_FORECAST_URL, params=params, timeout=10)
        if response.status_code != 200: return None
        return process_openmeteo_data(response.json())
    except Exception: return None

def fetch_weather_gfs(target_date):
    headers = {'Content-Type': 'application/json', 'token': API_TOKEN}
    time_str = target_date.strftime("%Y-%m-%d 00:00:00")
    req_body = {
        "time": time_str, 'timezone': 8, 'lon': LOCATION['lon'], 'lat': LOCATION['lat'],
        "mete_vars": ["t2m@C", "d2m@C", "ws10m", "wd10m", "dswrf", "tp", "rh", "skt@C", "tcc"]
    }
    try:
        response = requests.post(GFS_API_URL, headers=headers, json=req_body, timeout=10)
        if response.status_code != 200: return None
        res_json = response.json()
        if 'data' not in res_json or 'data' not in res_json['data']: return None
        
        data_block = res_json['data']
        values = data_block['data'][0]['values']
        timestamps = data_block['timestamp']
        mete_vars = data_block['mete_var']
        df = pd.DataFrame(values, index=pd.to_datetime(timestamps), columns=mete_vars)
        df = df.rename(columns={"t2m@C": "t2m", "d2m@C": "d2m", "skt@C": "skt"})
        
        target_day_str = target_date.strftime("%Y-%m-%d")
        day_df = df[df.index.strftime("%Y-%m-%d") == target_day_str].copy()
        if day_df.empty: return None
            
        day_df["wind_dir"] = day_df["wd10m"].apply(deg_to_cn_wind_dir)
        day_df["weather_type"] = day_df.apply(get_weather_type_row, axis=1)
        day_df["rain_prob"] = day_df.apply(rain_probability_row, axis=1)
        day_df["uv_index"] = (day_df["dswrf"] / 25.0).clip(0, 15).round(1)
        day_df["apparent_temp"] = day_df.apply(lambda r: r["t2m"] - 0.55 * (1 - r["rh"]/100) * (r["t2m"] - 14.5) if r["t2m"] > 20 else r["t2m"], axis=1)

        summary = {
            "max_temp": float(day_df["t2m"].max()),
            "min_temp": float(day_df["t2m"].min()),
            "avg_temp": float(day_df["t2m"].mean()),
            "weather_type": day_df["weather_type"].mode()[0] if not day_df["weather_type"].empty else "未知",
            "max_rain_prob": int(day_df["rain_prob"].max()),
            "temps": day_df["t2m"].tolist(),
            "weather_types": day_df["weather_type"].tolist(),
            "rain_probs": day_df["rain_prob"].tolist(),
            "humidities": day_df["rh"].tolist(),
            "apparent_temps": day_df["apparent_temp"].tolist(),
            "wind_speeds": day_df["ws10m"].tolist(),
            "wind_directions": day_df["wind_dir"].tolist(),
            "cloud_covers": day_df["tcc"].tolist(),
            "uv_indices": day_df["uv_index"].tolist(),
            "source": "GFS"
        }
        return summary
    except Exception: return None

def fetch_weather_for_date(target_date):
    today = datetime.date.today()
    if target_date < today - datetime.timedelta(days=2):
        return fetch_weather_openmeteo_archive(target_date)
    else:
        data = fetch_weather_openmeteo_forecast(target_date)
        if data:
             data["source"] = "OpenMeteo-Forecast"
             return data
        return fetch_weather_gfs(target_date)
