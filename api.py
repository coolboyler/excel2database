# api.py

from io import BytesIO
import json
import time
from fastapi import FastAPI, Query, UploadFile, File, Form, HTTPException, BackgroundTasks, Request, logger
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import os
import glob
import shutil
from typing import List, Optional
import numpy as np
import pandas as pd
from sqlalchemy import text
import uvicorn
import datetime
from pred_reader import PowerDataImporter
from database import DatabaseManager

app = FastAPI(
    title="Excel2SQL API",
    description="API for importing Excel data to SQL database",
    version="1.0.0"
)

# 挂载静态文件
app.mount("/static", StaticFiles(directory="static"), name="static")

# 设置模板
templates = Jinja2Templates(directory="templates")

# 初始化导入器和数据库管理器
importer = PowerDataImporter()
db_manager = DatabaseManager()

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    """返回前端页面"""
    return templates.TemplateResponse("index.html", {"request": request})

# 新增：表查询页面
@app.get("/table_query", response_class=HTMLResponse)
async def table_query_page(request: Request, table_name: str):
    """返回表查询页面"""
    return templates.TemplateResponse("table_query.html", {"request": request, "table_name": table_name})

# 新增：联表查询页面
@app.get("/join_query", response_class=HTMLResponse)
async def join_query_page(request: Request):
    """返回联表查询页面"""
    return templates.TemplateResponse("join_query.html", {"request": request})

@app.get("/health")
async def health_check():
    """健康检查接口"""
    db_status = db_manager.test_connection()
    return {
        "status": "healthy" if db_status else "unhealthy",
        "database": "connected" if db_status else "disconnected"
    }

@app.get("/files")
async def list_files():
    """列出data目录中的所有Excel文件"""
    data_folder = "data"
    os.makedirs(data_folder, exist_ok=True)
    excel_files = glob.glob(os.path.join(data_folder, "*.xlsx"))
    excel_files.sort(reverse=True)  # 按文件名倒序排列（最新日期在前）
    
    return {
        "total": len(excel_files),
        "files": [os.path.basename(file) for file in excel_files]
    }

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """上传Excel文件到data目录"""
    data_folder = "data"
    os.makedirs(data_folder, exist_ok=True)
    
    # 检查文件类型
    if not file.filename.endswith(('.xlsx')):
        raise HTTPException(status_code=400, detail="只支持.xlsx格式的Excel文件")
    
    # 保存文件
    file_path = os.path.join(data_folder, file.filename)
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    return {"filename": file.filename, "status": "uploaded"}

import re
from pydantic import BaseModel

class SimilarDayRequest(BaseModel):
    target_date: str
    date_type: Optional[str] = None
    weights: Optional[dict] = None

@app.post("/api/similar-day")
async def find_similar_days(request: SimilarDayRequest):
    """
    查找相似日
    匹配维度：负荷预测、天气、温度、B类占比、新能源D日预测、日前电价
    """
    try:
        target_date_str = request.target_date
        weights = request.weights or {}
        
        # 默认权重
        w_load = float(weights.get('load', 0.4))
        w_weather = float(weights.get('weather', 0.1))
        w_temp = float(weights.get('temp', 0.1))
        w_b_ratio = float(weights.get('b_ratio', 0.15))
        w_ne = float(weights.get('ne', 0.1))
        w_price = float(weights.get('price', 0.1))
        w_date = float(weights.get('date', 0.05)) # 日期衰减系数
        
        # 新增权重
        w_month = float(weights.get('month', 0.15)) # 默认考虑月份相似性（二进制：同月=0，不同月=1）
        w_weekday = float(weights.get('weekday', 0.15)) # 默认考虑星期几相似性（二进制：同星期几=0，不同=1）

        # 1. 获取所有缓存数据
        table_name = "cache_daily_hourly"
        with db_manager.engine.connect() as conn:
            # 检查表是否存在
            tables = db_manager.get_tables()
            if table_name not in tables:
                return {"error": "缓存表不存在，请先生成缓存"}

            # 获取全量数据
            # 我们需要以下字段: 
            # record_date, hour, load_forecast, weather, temperature, 
            # class_b_forecast, spot_ne_d_forecast, price_da
            
            # 构建查询字段
            fields = [
                "record_date", "hour", 
                "load_forecast", "weather", "temperature",
                "class_b_forecast", "spot_ne_d_forecast", "price_da"
            ]
            
            # 检查字段是否存在 (防止报错)
            # 简单起见，使用 SELECT *，然后在 Pandas 里处理
            df = pd.read_sql(f"SELECT * FROM {table_name}", conn)

        if df.empty:
            return {"error": "缓存表中无数据"}

        # 转换日期格式
        df['record_date'] = pd.to_datetime(df['record_date']).dt.strftime('%Y-%m-%d')
        
        # 2. 提取目标日数据
        target_df = df[df['record_date'] == target_date_str].sort_values('hour')
        
        if target_df.empty:
            return {"error": f"目标日期 {target_date_str} 无数据，请先导入预测数据"}

        # 获取目标日期类型
        target_day_type = target_df['day_type'].iloc[0] if 'day_type' in target_df.columns else ''

        # 3. 数据预处理
        # 需要将长表(long)转为宽表(wide)，或者直接按日期分组计算
        
        # 辅助函数：计算两个向量的距离 (MAPE 或 归一化欧氏距离)
        # 这里使用 MAPE (平均绝对百分比误差) 的变体作为差异度量
        
        # 准备历史数据 (必须是目标日之前的日期)
        history_df = df[df['record_date'] < target_date_str].copy()
        
        # 调试：显示目标日期类型（不再强制过滤）
        print(f"[DEBUG] 目标日类型: {target_day_type or '无类型'}")
        # 不再强制过滤日期类型，允许匹配所有历史数据
        # 用户可通过设置月份/星期几权重为0来禁用相关过滤
        
        # 必须有24小时数据的日期才参与计算
        print(f"[DEBUG] 历史数据天数（24小时过滤前）: {len(history_df['record_date'].unique())}")
        valid_dates = history_df.groupby('record_date').count()['hour']
        valid_dates = valid_dates[valid_dates == 24].index.tolist()
        history_df = history_df[history_df['record_date'].isin(valid_dates)]
        print(f"[DEBUG] 历史数据天数（24小时过滤后）: {len(history_df['record_date'].unique())}")
        
        if history_df.empty:
            return {"error": "没有足够的历史数据进行匹配"}

        # ---------------------------
        # 计算各项差异
        # ---------------------------
        
        results = []
        print(f"[DEBUG] 权重配置 - load:{w_load}, temp:{w_temp}, weather:{w_weather}, "
              f"b_ratio:{w_b_ratio}, ne:{w_ne}, price:{w_price}, "
              f"date:{w_date}, month:{w_month}, weekday:{w_weekday}")
        target_date_obj = datetime.datetime.strptime(target_date_str, "%Y-%m-%d").date()
        print(f"[DEBUG] 目标日期: {target_date_str}, 月份: {target_date_obj.month}, 星期几: {target_date_obj.weekday()}(0=周一)")

        # 预计算目标向量
        t_load = target_df['load_forecast'].fillna(0).values
        t_temp = target_df['temperature'].fillna(0).values
        t_price = target_df['price_da'].fillna(0).values
        
        # B类占比
        t_b = target_df['class_b_forecast'].fillna(0).values
        # 避免除以0
        t_load_safe = np.where(t_load == 0, 1, t_load)
        t_b_ratio = t_b / t_load_safe
        
        # 新能源D日
        # 优先使用 spot_ne_d_forecast，如果没有则尝试用 new_energy_forecast
        if 'spot_ne_d_forecast' in target_df.columns and target_df['spot_ne_d_forecast'].sum() > 0:
            t_ne = target_df['spot_ne_d_forecast'].fillna(0).values
        elif 'new_energy_forecast' in target_df.columns:
            t_ne = target_df['new_energy_forecast'].fillna(0).values
        else:
            t_ne = np.zeros(24)

        # 天气 (字符串数组)
        t_weather = target_df['weather'].fillna("").values
        
        # 计算目标日期的统计信息
        target_weather_type = ""
        if len(t_weather) > 12:
            target_weather_type = t_weather[12]  # 取中午时段的天气作为代表
        elif len(t_weather) > 0:
            target_weather_type = t_weather[0]   # 如果没有12点数据，取第一个
        
        target_avg_temp = float(np.mean(t_temp)) if len(t_temp) > 0 else 0.0
        target_avg_load = float(np.mean(t_load)) if len(t_load) > 0 else 0.0
        target_avg_price = float(np.mean(t_price)) if len(t_price) > 0 else 0.0
        target_avg_b_ratio = float(np.mean(t_b_ratio)) if len(t_b_ratio) > 0 else 0.0
        target_avg_ne = float(np.mean(t_ne)) if len(t_ne) > 0 else 0.0

        # 遍历历史日期
        # 为了加速，可以使用 groupby Apply，但循环简单直观
        for date_val, group in history_df.groupby('record_date'):
            group = group.sort_values('hour')
            
            # 1. 负荷差异 (MAPE)
            h_load = group['load_forecast'].fillna(0).values
            # 如果负荷为空，跳过
            if np.sum(h_load) == 0:
                diff_load = 1.0 # 最大差异
            else:
                # MAPE: mean(abs(t - h) / t) -> 但 t 可能为0，且我们要的是相似度
                # 使用 归一化欧氏距离: dist / (norm(t) + norm(h)) 或 simple MAPE
                # 简单处理：mean(abs(diff)) / mean(target)
                mean_target = np.mean(t_load) if np.mean(t_load) > 0 else 1
                diff_load = np.mean(np.abs(t_load - h_load)) / mean_target
            
            # 2. 温度差异 (RMSE + 最高最低对比)
            h_temp = group['temperature'].fillna(0).values
            diff_temp = np.sqrt(np.mean((t_temp - h_temp)**2))
            # 最高温度差异
            max_diff = np.max(t_temp) - np.max(h_temp)
            diff_temp_max = abs(max_diff)
            # 最低温度差异
            min_diff = np.min(t_temp) - np.min(h_temp)
            diff_temp_min = abs(min_diff)
            # 综合温度差异归一化 (假设温差10度算大)
            diff_temp_norm = min((diff_temp / 10.0 + diff_temp_max / 10.0 + diff_temp_min / 10.0) / 3.0, 1.0)
            
            # 3. B类占比差异
            h_b = group['class_b_forecast'].fillna(0).values
            h_load_safe = np.where(h_load == 0, 1, h_load)
            h_b_ratio = h_b / h_load_safe
            diff_b_ratio = np.mean(np.abs(t_b_ratio - h_b_ratio)) # 本身就是比例，直接差值
            
            # 4. 新能源差异
            # 同样处理列名
            if 'spot_ne_d_forecast' in group.columns and group['spot_ne_d_forecast'].sum() > 0:
                h_ne = group['spot_ne_d_forecast'].fillna(0).values
            elif 'new_energy_forecast' in group.columns:
                h_ne = group['new_energy_forecast'].fillna(0).values
            else:
                h_ne = np.zeros(24)
            
            mean_ne_target = np.mean(t_ne) if np.mean(t_ne) > 0 else 1
            diff_ne = np.mean(np.abs(t_ne - h_ne)) / mean_ne_target
            
            # 5. 价格差异
            h_price = group['price_da'].fillna(0).values
            mean_price_target = np.mean(t_price) if np.mean(t_price) > 0 else 1
            diff_price = np.mean(np.abs(t_price - h_price)) / mean_price_target
            
            # 6. 天气差异 (不匹配的小时数比例)
            h_weather = group['weather'].fillna("").values
            # 简单比较字符串是否相等
            diff_weather = np.mean(t_weather != h_weather)
            
            # 7. 日期权重 (越近越好)
            # 计算天数差
            hist_date_obj = datetime.datetime.strptime(date_val, "%Y-%m-%d").date()
            days_diff = abs((target_date_obj - hist_date_obj).days)
            # 衰减因子: 1 - exp(-k * days) -> 距离
            # 或者 距离增加: days_diff / 365
            date_penalty = min(days_diff / 365.0, 1.0)
            
            # 8. 月份差异 (二进制: 同月=0, 不同月=1)
            target_month = target_date_obj.month
            hist_month = hist_date_obj.month
            diff_month = 0.0 if target_month == hist_month else 1.0
            
            # 9. 星期几差异 (二进制: 同为星期几=0, 不同=1)
            target_weekday = target_date_obj.weekday()  # Monday=0, Sunday=6
            hist_weekday = hist_date_obj.weekday()
            diff_weekday = 0.0 if target_weekday == hist_weekday else 1.0
            
            # 总差异得分 (越小越好)
            # 各项 diff 都在 [0, 1] 左右 (MAPE可能大于1，但通常在0-0.5)
            total_score = (
                w_load * diff_load +
                w_temp * diff_temp_norm +
                w_b_ratio * diff_b_ratio +
                w_ne * diff_ne +
                w_price * diff_price +
                w_weather * diff_weather +
                w_date * date_penalty +
                w_month * diff_month +
                w_weekday * diff_weekday
            )
            
            results.append({
                "date": date_val,
                "score": total_score,
                "details": {
                    "diff_load": float(diff_load),
                    "diff_temp": float(diff_temp),
                    "diff_temp_max": float(diff_temp_max),
                    "diff_temp_min": float(diff_temp_min),
                    "diff_weather": float(diff_weather),
                    "diff_price": float(diff_price),
                    "diff_b_ratio": float(diff_b_ratio),
                    "diff_ne": float(diff_ne),
                    "diff_month": float(diff_month),
                    "diff_weekday": float(diff_weekday)
                },
                # 返回一些用于展示的数据
                "load_curve": h_load.tolist(),
                "price_curve": h_price.tolist(),
                "temp_avg": float(np.mean(h_temp)),
                "weather_type": h_weather[12] if len(h_weather) > 12 else "", # 取中午天气作为代表
                "day_type": group['day_type'].iloc[0] if 'day_type' in group.columns else ""
            })
            
        # 排序并返回前5
        results.sort(key=lambda x: x['score'])
        top_matches = results[:5]
        
        # 转换得分为相似度 (1 / (1 + score)) 或者 (1 - score)
        for r in top_matches:
            r['similarity_score'] = max(0, 1 - r['score']) # 简单线性映射
            
        return {
            "target_date": target_date_str,
            "target_day_type": target_day_type,
            "target_weather_type": target_weather_type,
            "target_stats": {
                "avg_temp": target_avg_temp,
                "avg_load": target_avg_load,
                "avg_price": target_avg_price,
                "avg_b_ratio": target_avg_b_ratio,
                "avg_ne": target_avg_ne
            },
            "target_load_curve": t_load.tolist(),
            "target_price_curve": t_price.tolist(),
            "matches": top_matches
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/api/update-weather")
async def update_weather(background_tasks: BackgroundTasks):
    """手动触发天气数据更新"""
    try:
        import calendar_weather
        today = datetime.date.today()
        # 更新最近30天和未来15天的数据
        start_date = today - datetime.timedelta(days=30)
        end_date = today + datetime.timedelta(days=15)
        
        # 使用后台任务执行，避免阻塞
        def run_update():
            print(f"🌦️ 开始更新天气数据: {start_date} -> {end_date}")
            # update_calendar 内部现在会自动调用 update_price_cache_for_date(..., only_weather=True)
            # 从而实现“只更新天气表，并存入缓存表，不更新价差数据”
            calendar_weather.update_calendar(start_date, end_date)
            print("✅ 天气数据及缓存更新完成")
            
        background_tasks.add_task(run_update)
        
        return {"status": "success", "message": f"天气更新任务已启动 ({start_date} 至 {end_date})"}
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"启动天气更新任务失败: {str(e)}")

@app.post("/import")
async def import_file(filename: str = Form(...), background_tasks: BackgroundTasks = BackgroundTasks()):
    """导入指定的Excel文件到数据库"""
    data_folder = "data"
    file_path = os.path.join(data_folder, filename)
    
    # 检查文件是否存在
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"文件 {filename} 不存在")
    
    # 定义正则表达式模式
    dated_realtime_pattern = r'\d{4}-\d{2}-\d{2}实时节点电价查询'
    dated_dayahead_pattern = r'\d{4}-\d{2}-\d{2}日前节点电价查询'

    if "负荷实际信息" in filename or "负荷预测信息" in filename:
        method = importer.import_power_data
    # elif "信息披露(区域)查询实际信息" in filename:
    #     method = importer.import_custom_excel
    # elif "信息披露(区域)查询预测信息" in filename:
    #     method = importer.import_custom_excel_pred
    elif "信息披露查询预测信息" in filename:
        method = importer.import_imformation_pred
    elif "信息披露查询实际信息" in filename:
        method = importer.import_imformation_true    
    # 先处理带日期的特殊版本
    elif re.search(dated_realtime_pattern, filename) or re.search(dated_dayahead_pattern, filename):
        method = importer.import_point_data_new
    # 然后处理不带日期的通用版本
    elif "实时节点电价查询" in filename or "日前节点电价查询" in filename:
        method = importer.import_point_data
    else:
        raise HTTPException(status_code=400, detail=f"无匹配的导入规则: {filename}")

    # 执行同步导入
    result = method(file_path)
    
    # 检查结果是否为 False (表示导入失败)
    if result is False:
        raise HTTPException(status_code=500, detail=f"导入失败: {filename}，请检查文件格式或日志")

    # [新增逻辑] 自动触发缓存更新
    try:
        # 尝试从文件名提取日期
        date_match = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
        if not date_match:
             date_match = re.search(r"(\d{8})", filename)
        
        target_date = None
        if date_match:
            d_str = date_match.group(1)
            if len(d_str) == 8:
                target_date = f"{d_str[:4]}-{d_str[4:6]}-{d_str[6:]}"
            else:
                target_date = d_str
        
        # 只有在导入了节点电价相关文件或信息披露文件，且能提取到日期时，才触发更新
        if target_date and ("节点电价" in filename or "信息披露" in filename):
            print(f"🚀 自动触发缓存更新任务: {target_date}")
            background_tasks.add_task(update_price_cache_for_date, target_date)
            
    except Exception as e:
        print(f"⚠️ 自动触发缓存更新失败: {e}")

    if method == importer.import_imformation_pred:
        # 结果可能是单个四元组 (success, table, count, preview)
        # 也可能是多个四元组的元组 ((s1,t1,c1,p1), (s2,t2,c2,p2))
        
        # 情况1: 单个结果 (4个元素)
        if isinstance(result, tuple) and len(result) == 4 and not isinstance(result[0], tuple):
             success, table_name, record_count, preview_data = result
             
        # 情况2: 多个结果 (元组的元组)
        elif isinstance(result, tuple) and len(result) > 0 and isinstance(result[0], tuple):
             # 合并所有结果
             success = all(r[0] for r in result)
             table_name = ", ".join([str(r[1]) for r in result])
             record_count = sum(r[2] for r in result)
             # 合并预览数据 (取前几个)
             preview_data = []
             for r in result:
                 if r[3]:
                     preview_data.extend(r[3])
             preview_data = preview_data[:5] # 只保留前5条作为总预览
             
        else:
             raise HTTPException(status_code=500, detail=f"导入返回格式错误: {result}")
    
    elif method == importer.import_imformation_true:
         if isinstance(result, tuple) and len(result) == 4:
             success, table_name, record_count, preview_data = result
         # 处理可能返回None的情况（例如导入过程报错了）
         elif result is None:
             raise HTTPException(status_code=500, detail="导入失败: 内部错误")
         # 处理返回多表结果的情况 (tuple of tuples)
         elif isinstance(result, tuple) and len(result) > 0 and isinstance(result[0], tuple):
             # 合并所有结果
             success = all(r[0] for r in result)
             table_name = ", ".join([str(r[1]) for r in result])
             record_count = sum(r[2] for r in result)
             # 合并预览数据 (取前几个)
             preview_data = []
             for r in result:
                 if r[3]:
                     preview_data.extend(r[3])
             preview_data = preview_data[:5]
         else:
             # 如果是其他格式，尝试打印一下看看
             print(f"DEBUG: import_imformation_true returned: {type(result)} - {result}")
             raise HTTPException(status_code=500, detail=f"导入返回格式错误: {result}")

    elif method == importer.import_custom_excel:
        if isinstance(result, tuple) and len(result) == 3:
            # 解包三个结果元组
            (success1, table_name1, record_count1, preview_data1), (success2, table_name2, record_count2, preview_data2),(success3,table_name3,record_count3,preview_data3) = result
            # 合并结果，这里我们使用三个结果的组合
            success = success1 and success2 and success3
            table_name = f"{table_name1}, {table_name2}, {table_name3}"
            record_count = record_count1 + record_count2 + record_count3
            preview_data = preview_data1 + preview_data2 + preview_data3
        else:
             raise HTTPException(status_code=500, detail=f"导入返回格式错误: {result}")

    elif method == importer.import_custom_excel_pred:
        if isinstance(result, tuple) and len(result) == 4:
            (success1, table_name1, record_count1, preview_data1), (success2, table_name2, record_count2, preview_data2), (success4, table_name4, record_count4, preview_data4), (success5, table_name5, record_count5, preview_data5) = result
            # 合并结果，这里我们使用四个结果的组合
            success = success1 and success2 and success4 and success5
            table_name = f"{table_name1}, {table_name2}, {table_name4}, {table_name5}"
            record_count = record_count1 + record_count2 + record_count4 + record_count5 
            preview_data = preview_data1 + preview_data2 + preview_data4 + preview_data5 
        else:
             raise HTTPException(status_code=500, detail=f"导入返回格式错误: {result}")
    else:
        # 其他导入方法的常规处理
        if isinstance(result, tuple) and len(result) == 4:
            success, table_name, record_count, preview_data = result
        else:
            raise HTTPException(status_code=500, detail=f"导入返回格式错误: {result}")
        
    if success:
        return {
            "filename": filename, 
            "status": "imported", 
            "table_name": table_name, 
            "record_count": record_count,
            "preview_data": preview_data
        }
    else:
        raise HTTPException(status_code=500, detail=f"导入失败: {filename}")

@app.get("/tables")
async def get_tables():
    """获取所有数据表"""
    tables = db_manager.get_tables()
    return {"tables": tables}

@app.get("/tables/{table_name}")
async def get_table_data(table_name: str, limit: int = 5):
    """获取指定表的数据"""
    result = db_manager.get_table_data(table_name, limit)
    return result

# 新增：获取表结构信息
@app.get("/tables/{table_name}/schema")
async def get_table_schema(table_name: str):
    """获取指定表的结构信息"""
    try:
        with db_manager.engine.connect() as conn:
            result = conn.execute(text(f"DESCRIBE {table_name}"))
            schema = []
            for row in result:
                schema.append({
                    "field": row[0],
                    "type": row[1],
                    "null": row[2],
                    "key": row[3],
                    "default": row[4],
                    "extra": row[5]
                })
            return {"schema": schema}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取表结构失败: {str(e)}")

# 新增：查询表数据接口
@app.get("/tables/{table_name}/query")
async def query_table_data(table_name: str, 
                          offset: int = 0, 
                          limit: int = 20,
                          conditions: str = None):
    """查询指定表的数据，支持多条件查询
    conditions: JSON字符串，格式如 [{"column": "col1", "operator": "=", "value": "val1"}, 
                                   {"column": "col2", "operator": ">", "value": "val2"}]
    """
    try:
        with db_manager.engine.connect() as conn:
            # 构建查询条件
            where_clauses = []
            params = {}
            
            if conditions:
                import json
                try:
                    condition_list = json.loads(conditions)
                    if isinstance(condition_list, list):
                        for i, cond in enumerate(condition_list):
                            column = cond.get("column")
                            operator = cond.get("operator")
                            value = cond.get("value")
                            
                            if column and operator and value is not None:
                                # 简单的SQL注入防护
                                allowed_operators = ['=', '!=', '>', '<', '>=', '<=', 'LIKE']
                                if operator not in allowed_operators:
                                    raise HTTPException(status_code=400, detail=f"不支持的操作符: {operator}")
                                
                                param_name = f"value_{i}"
                                if operator == 'LIKE':
                                    where_clauses.append(f"{column} LIKE :{param_name}")
                                    params[param_name] = f"%{value}%"
                                else:
                                    where_clauses.append(f"{column} {operator} :{param_name}")
                                    # 尝试转换数值类型
                                    try:
                                        params[param_name] = int(value)
                                    except ValueError:
                                        try:
                                            params[param_name] = float(value)
                                        except ValueError:
                                            params[param_name] = value
                except json.JSONDecodeError:
                    raise HTTPException(status_code=400, detail="条件格式错误")
            
            # 构建WHERE子句
            where_clause = ""
            if where_clauses:
                where_clause = "WHERE " + " AND ".join(where_clauses)
            
            # 获取总记录数
            count_query = f"SELECT COUNT(*) FROM {table_name} {where_clause}"
            count_result = conn.execute(text(count_query), params)
            total_count = count_result.scalar()
            
            # 获取分页数据
            # 默认添加排序：优先按record_date倒序，其次按id倒序
            order_clause = ""
            # 简单检查表结构中是否有record_date列（可以通过查询一行数据或describe，这里简化处理，假设大部分表都有id）
            # 更稳妥的方式是直接尝试ORDER BY id DESC，如果报错则忽略
            # 但由于我们要执行SQL，这里最好直接拼接到SQL中。
            # 为了兼容性，我们先不强制加ORDER BY，除非用户没有指定排序（当前接口不支持指定排序）
            # 我们可以默认加 ORDER BY id DESC，因为大部分表都有id主键
            
            # 检查是否有id列或record_date列比较耗时，这里直接尝试按id倒序，因为我们的建表语句都包含id
            data_query = f"SELECT * FROM {table_name} {where_clause} ORDER BY id DESC LIMIT :limit OFFSET :offset"
            
            params.update({"limit": limit, "offset": offset})
            try:
                data_result = conn.execute(text(data_query), params)
            except Exception:
                # 如果失败（例如没有id列），回退到无排序
                data_query = f"SELECT * FROM {table_name} {where_clause} LIMIT :limit OFFSET :offset"
                data_result = conn.execute(text(data_query), params)
            
            data = []
            for row in data_result:
                row_dict = dict(row._mapping)
                data.append(row_dict)
            
            return {
                "data": data,
                "total": total_count,
                "offset": offset,
                "limit": limit
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询数据失败: {str(e)}")

@app.get("/tables/{table_name}/export")
async def export_table_data(table_name: str,
                           conditions: str = None):
    """导出指定表的数据为Excel格式，支持多条件查询
    conditions: JSON字符串，格式如 [{"column": "col1", "operator": "=", "value": "val1"}, 
                                   {"column": "col2", "operator": ">", "value": "val2"}]
    """
    try:
        print(f"导出请求开始: table_name={table_name}, conditions={conditions}")
        
        with db_manager.engine.connect() as conn:
            # 构建查询条件
            where_clauses = []
            params = {}
            
            if conditions:
                import json
                try:
                    condition_list = json.loads(conditions)
                    if isinstance(condition_list, list):
                        for i, cond in enumerate(condition_list):
                            column = cond.get("column")
                            operator = cond.get("operator")
                            value = cond.get("value")
                            
                            if column and operator and value is not None:
                                # 简单的SQL注入防护
                                allowed_operators = ['=', '!=', '>', '<', '>=', '<=', 'LIKE']
                                if operator not in allowed_operators:
                                    raise HTTPException(status_code=400, detail=f"不支持的操作符: {operator}")
                                
                                param_name = f"value_{i}"
                                if operator == 'LIKE':
                                    where_clauses.append(f"{column} LIKE :{param_name}")
                                    params[param_name] = f"%{value}%"
                                else:
                                    where_clauses.append(f"{column} {operator} :{param_name}")
                                    # 尝试转换数值类型
                                    try:
                                        params[param_name] = int(value)
                                    except ValueError:
                                        try:
                                            params[param_name] = float(value)
                                        except ValueError:
                                            params[param_name] = value
                except json.JSONDecodeError:
                    raise HTTPException(status_code=400, detail="条件格式错误")
            
            # 构建WHERE子句
            where_clause = ""
            if where_clauses:
                where_clause = "WHERE " + " AND ".join(where_clauses)
            
            # 获取所有数据
            data_query = f"SELECT * FROM {table_name} {where_clause}"
            print(f"执行查询: {data_query}, 参数: {params}")
            data_result = conn.execute(text(data_query), params)
            
            data = []
            for row in data_result:
                row_dict = dict(row._mapping)
                data.append(row_dict)
            
            print(f"查询结果数量: {len(data)}")
            if len(data) > 0:
                print(f"前几条数据示例: {data[:2]}")
            
            # 如果没有数据，返回空Excel
            if not data:
                import pandas as pd
                import numpy as np
                from io import BytesIO
                df = pd.DataFrame()
                output = BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False)
                output.seek(0)
                
                from fastapi.responses import StreamingResponse
                return StreamingResponse(
                    iter([output.getvalue()]),
                    media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    headers={"Content-Disposition": f"attachment; filename={table_name}.xlsx"}
                )
            
            # 转换为DataFrame进行处理
            import pandas as pd
            import numpy as np
            from io import BytesIO
            import os
            from datetime import datetime
            
            df = pd.DataFrame(data)
            print(f"DataFrame列: {df.columns.tolist()}")
            print(f"DataFrame形状: {df.shape}")
            if len(df) > 0:
                print(f"DataFrame前几行:\n{df.head(2)}")
            
            # 删除id列（如果存在）
            if 'id' in df.columns:
                df = df.drop(columns=['id'])
                print("已删除id列")
            
            # 检查是否包含必要的列
            required_columns = ['channel_name', 'record_date', 'record_time', 'value', 'sheet_name']
            if not all(col in df.columns for col in required_columns):
                print(f"缺少必要列，当前列: {df.columns.tolist()}")
                print("使用原始导出方式")
                # 如果不包含必要列，使用原始导出方式
                output = BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False)
                output.seek(0)
                
                # 生成文件名
                record_date = df['record_date'].iloc[0] if 'record_date' in df.columns and len(df) > 0 else 'unknown'
                data_type = df['type'].iloc[0] if 'type' in df.columns and len(df) > 0 else 'unknown'
                
                # 格式化record_date为字符串
                if hasattr(record_date, 'strftime'):
                    record_date_str = record_date.strftime('%Y-%m-%d')
                else:
                    record_date_str = str(record_date)
                
                filename = f"{record_date_str}_{data_type}.xlsx"
                
                import urllib.parse
                encoded_filename = urllib.parse.quote(filename)
                from fastapi.responses import StreamingResponse
                return StreamingResponse(
                    iter([output.getvalue()]),
                    media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    headers={
                        "Content-Disposition": f"attachment; filename*=UTF-8''{encoded_filename}"
                    }
                )
            
            # 类似preHandle.py的处理方式
            # 提取唯一的sheet_name（假设数据中sheet_name唯一）
            sheet_name = df['sheet_name'].unique()[0] if len(df['sheet_name'].unique()) > 0 else 'Sheet1'
            # 提取唯一的日期（假设数据中日期唯一）
            record_date = df['record_date'].unique()[0] if len(df['record_date'].unique()) > 0 else pd.Timestamp.now().date()
            
            # 格式化日期为YYYY-MM-DD
            if hasattr(record_date, 'strftime'):
                record_date_str = record_date.strftime('%Y-%m-%d')
            else:
                record_date_str = str(record_date)
            
            # 处理文件名特殊字符（避免斜杠、空格等导致保存失败）
            sheet_name_clean = str(sheet_name).replace('/', '_').replace('\\', '_').replace(' ', '')
            # 构造文件名：{sheet_name}({日期})_小时.xlsx
            filename = f"{sheet_name_clean}({record_date_str})_小时.xlsx"
            print(f"生成文件名: {filename}")
            
            # 检查record_time格式并处理
            print(f"record_time示例值: {df['record_time'].head()}")
            
            # 转换record_time为小时（处理各种可能的格式）
            def extract_hour(time_value):
                if pd.isna(time_value):
                    return None
                if isinstance(time_value, str):
                    if ':' in time_value:
                        # 格式如 "01:00", "1:00"
                        return int(time_value.split(':')[0])
                    else:
                        # 可能是数字字符串如 "100" 表示 01:00
                        try:
                            time_int = int(time_value)
                            return time_int // 100
                        except:
                            return None
                elif isinstance(time_value, (int, float)):
                    # 数字格式如 100 表示 01:00
                    return int(time_value) // 100
                else:
                    # timedelta或其他格式
                    try:
                        # 如果是timedelta对象
                        hours = time_value.seconds // 3600
                        return hours
                    except:
                        return None
            
            # 应用小时提取函数
            df['hour'] = df['record_time'].apply(extract_hour)
            print(f"提取的小时列示例: {df['hour'].head()}")
            
            # 删除hour为NaN的行
            df = df.dropna(subset=['hour'])
            print(f"删除无效小时后DataFrame形状: {df.shape}")
            
            # 生成电站级透视表
            if len(df) > 0:
                print("开始创建透视表")
                pivot_df = pd.pivot_table(
                    df,
                    index=['channel_name', 'record_date'],
                    columns='hour',
                    values='value',
                    aggfunc='mean'
                )
                print(f"透视表创建完成，形状: {pivot_df.shape}")
                print(f"透视表列: {pivot_df.columns.tolist()}")
                
                # 重新索引确保有24小时列
                pivot_df = pivot_df.reindex(columns=range(24), fill_value=np.nan)
                pivot_df.columns = [f'{int(h)}:00' for h in pivot_df.columns]
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
                # 确保所有小时列都存在
                for col in hour_columns:
                    if col not in pivot_df.columns:
                        pivot_df[col] = np.nan
                
                # 在计算平均值前，确保所有列为数值类型
                for col in hour_columns:
                    pivot_df[col] = pd.to_numeric(pivot_df[col], errors='coerce')
                
                final_df = pivot_df
                print(f"最终DataFrame形状: {final_df.shape}")
                print(f"最终DataFrame列: {final_df.columns.tolist()}")
                if len(final_df) > 0:
                    print(f"最终DataFrame前几行:\n{final_df.head()}")
            else:
                # 如果处理后没有数据，创建空的DataFrame
                print("处理后没有有效数据，创建空DataFrame")
                columns = ['节点名称', '日期', '单位'] + [f'{h}:00' for h in range(24)]
                final_df = pd.DataFrame(columns=columns)
            
            # 确保created文件夹存在
            created_folder = "created"
            if not os.path.exists(created_folder):
                os.makedirs(created_folder)
                print(f"创建文件夹: {created_folder}")
            
            # 生成文件名（带时间戳避免重复）
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_name_with_timestamp = f"{sheet_name_clean}_{timestamp}.xlsx"
            file_path = os.path.join(created_folder, file_name_with_timestamp)
            print(f"生成文件路径: {file_path}")
            
            # 将处理后的final_df保存到服务器文件夹
            print("开始生成Excel文件到服务器")
            try:
                # 使用openpyxl引擎直接导出
                with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                    final_df.to_excel(writer, index=False, sheet_name=sheet_name_clean[:31])
                print(f"Excel文件生成完成: {file_path}")
                
            except Exception as e:
                print(f"Excel文件生成失败: {e}")
                import traceback
                traceback.print_exc()
                
                # 回退到CSV格式
                file_name_with_timestamp = file_name_with_timestamp.replace('.xlsx', '.csv')
                file_path = os.path.join(created_folder, file_name_with_timestamp)
                final_df.to_csv(file_path, index=False)
                print(f"CSV文件生成完成: {file_path}")
            
            # 返回文件下载链接
            from fastapi.responses import JSONResponse
            download_url = f"/download/{file_name_with_timestamp}"
            return JSONResponse({
                "status": "success",
                "message": "文件生成成功",
                "download_url": download_url,
                "filename": file_name_with_timestamp
            })
            
    except HTTPException:
        raise
    except Exception as e:
        print(f"导出数据失败: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"导出数据失败: {str(e)}")

@app.get("/download/{filename}")
async def download_file(filename: str):
    """下载生成的文件"""
    import os
    from fastapi.responses import FileResponse
    from fastapi import HTTPException
    
    file_path = os.path.join("created", filename)
    
    # 检查文件是否存在
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="文件不存在")
    
    # 根据文件扩展名设置正确的媒体类型
    if filename.endswith('.xlsx'):
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    elif filename.endswith('.csv'):
        media_type = "text/csv"
    else:
        media_type = "application/octet-stream"
    
    return FileResponse(
        path=file_path,
        media_type=media_type,
        filename=filename
    )

@app.delete("/tables/{table_name}")
async def delete_table(table_name: str):
    """删除指定表"""
    success = db_manager.delete_table(table_name)
    if success:
        return {"status": "success", "message": f"表 {table_name} 已删除"}
    else:
        raise HTTPException(status_code=500, detail=f"删除表 {table_name} 失败")

@app.post("/import-all")
async def import_all_files(background_tasks: BackgroundTasks):
    """导入data目录中的所有Excel文件"""
    data_folder = "data"
    excel_files = glob.glob(os.path.join(data_folder, "*.xlsx"))
    
    if not excel_files:
        raise HTTPException(status_code=404, detail=f"在 {data_folder} 文件夹中未找到任何Excel文件")
    
    # 添加所有文件到后台任务
    for excel_file in excel_files:
        filename = os.path.basename(excel_file)
        # 修复：正确传递参数
        background_tasks.add_task(import_file, filename=filename)
    
    return {
        "total": len(excel_files),
        "files": [os.path.basename(file) for file in excel_files],
        "status": "importing"
    }

@app.delete("/files/{filename}")
async def delete_file(filename: str):
    """删除指定的Excel文件"""
    data_folder = "data"
    file_path = os.path.join(data_folder, filename)
    
    # 检查文件是否存在
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"文件 {filename} 不存在")
    
    # 删除文件
    try:
        os.remove(file_path)
        return {"filename": filename, "status": "deleted"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"删除文件失败: {str(e)}")

@app.delete("/files")
async def delete_all_files():
    """删除所有Excel文件"""
    data_folder = "data"
    if not os.path.exists(data_folder):
        raise HTTPException(status_code=404, detail="数据目录不存在")
    
    deleted_files = []
    for filename in os.listdir(data_folder):
        if filename.endswith(".xlsx"):
            file_path = os.path.join(data_folder, filename)
            try:
                os.remove(file_path)
                deleted_files.append(filename)
            except Exception as e:
                logger.error(f"删除文件 {filename} 失败: {e}")
    
    return {
        "message": f"成功删除 {len(deleted_files)} 个文件",
        "deleted_files": deleted_files
    }

@app.delete("/tables")
async def delete_all_tables():
    """删除所有数据库表"""
    try:
        # 获取所有表名
        tables = db_manager.get_tables()
        
        deleted_tables = []
        for table in tables:
            try:
                # 删除表
                db_manager.delete_table(table)
                deleted_tables.append(table)
            except Exception as e:
                print(f"删除表 {table} 失败: {e}")
        
        return {
            "message": f"成功删除 {len(deleted_tables)} 个表",
            "deleted_tables": deleted_tables
        }
    except Exception as e:
        print(f"删除所有表时出错: {e}")
        raise HTTPException(status_code=500, detail="删除所有表失败")

        return {
            "message": f"成功删除 {len(deleted_tables)} 个表",
            "deleted_tables": deleted_tables
        }
    except Exception as e:
        print(f"删除所有表时出错: {e}")
        raise HTTPException(status_code=500, detail="删除所有表失败")

@app.post("/api/generate-daily-hourly-cache")
async def generate_daily_hourly_cache():
    """
    生成所有日期的分时数据缓存
    (修改为：仅执行 init_weather 逻辑，即全量更新日历和天气，并同步缓存中的天气数据)
    """
    from sql_config import SQL_RULES
    from fastapi.concurrency import run_in_threadpool
    import calendar_weather
    
    try:
        # 1. 确定表结构 (保留建表逻辑，防止表不存在导致后续更新缓存失败)
        table_name = "cache_daily_hourly"
        
        # 构建字段列表
        # 基础字段
        columns_def = [
            "`record_date` DATE NOT NULL",
            "`hour` TINYINT NOT NULL",
            "`updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
        ]
        
        # 从 SQL_RULES 动态生成字段
        # 加上计算字段
        calc_fields = {
            "price_diff": "FLOAT COMMENT '价差'",
            "load_deviation": "FLOAT COMMENT '负荷偏差'",
            "new_energy_forecast": "FLOAT COMMENT '新能源预测总和'"
        }
        
        # 合并所有字段
        all_fields = {}
        
        # 添加规则中的字段
        for key, rule in SQL_RULES.items():
            field_name = key
            # 默认都是 FLOAT，除了日期/字符串类型
            if key in ['date', 'day_type', 'week_day', 'weather', 'wind_direction']:
                col_type = "VARCHAR(50)"
            else:
                col_type = "FLOAT"
            
            all_fields[field_name] = f"`{field_name}` {col_type} COMMENT '{rule.get('name', '')}'"
            
        # 添加计算字段
        for k, v in calc_fields.items():
            all_fields[k] = f"`{k}` {v}"
            
        # 组装 CREATE TABLE 语句
        cols_sql = ",\n".join(list(all_fields.values()) + columns_def)
        
        with db_manager.engine.begin() as conn:
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {cols_sql},
                PRIMARY KEY (`record_date`, `hour`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
            conn.execute(text(create_sql))
            print(f"✅ 缓存表 {table_name} 已就绪")

        # 2. 执行 init_weather 逻辑 (全量更新日历和天气)
        # 参考 init_calendar.py 的范围，或者覆盖较长的时间段
        start_date = datetime.date(2023, 1, 1)
        end_date = datetime.date(2027, 12, 31)
        
        print(f"🚀 开始执行全量天气初始化: {start_date} -> {end_date}")
        
        # 在线程池中运行，避免阻塞主线程
        await run_in_threadpool(calendar_weather.update_calendar, start_date, end_date)
        
        return {"status": "success", "message": f"全量天气及缓存更新完成 ({start_date} 至 {end_date})"}

    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"error": str(e)})

async def calculate_daily_hourly_data(date: str):
    """
    计算指定日期的分时数据（核心逻辑提取）
    返回: List[Dict] (24小时数据)
    """
    from sql_config import SQL_RULES, TABLE_SOURCE_POWER, TABLE_SOURCE_WEATHER
    try:
        target_date = pd.to_datetime(date).date()
        date_str = target_date.strftime("%Y%m%d")
        target_date_str = target_date.strftime("%Y-%m-%d")
        table_name_power = f"power_data_{date_str}"
        table_name_weather = "calendar_weather"
        
        tables = db_manager.get_tables()
        if table_name_power not in tables:
            return None
            
        hourly_data_lists = {h: {} for h in range(24)}
        daily_weather_data = {}
        
        with db_manager.engine.connect() as conn:
            # 1. 查电力数据
            for key, rule in SQL_RULES.items():
                if rule.get("source") == TABLE_SOURCE_POWER:
                    where_clause = rule["where"]
                    sql = text(f"SELECT record_time, value FROM {table_name_power} WHERE {where_clause}")
                    result = conn.execute(sql).fetchall()
                    
                    for row in result:
                        r_time = row[0]
                        val = float(row[1]) if row[1] is not None else 0
                        
                        if hasattr(r_time, 'total_seconds'):
                            hour = int(r_time.total_seconds() // 3600)
                        else:
                            continue
                            
                        if 0 <= hour <= 23:
                            hourly_data_lists[hour].setdefault(key, []).append(val)

        # 2. 查天气数据
        if table_name_weather in tables:
            with db_manager.engine.connect() as conn:
                sql = text(f"SELECT * FROM {table_name_weather} WHERE date = :d")
                row = conn.execute(sql, {"d": target_date_str}).fetchone()
                
                if row:
                    row_dict = dict(row._mapping)
                    weather_json = row_dict.get("weather_json")
                    if isinstance(weather_json, str):
                        try:
                            import json
                            weather_json = json.loads(weather_json)
                        except:
                            weather_json = {}
                    elif weather_json is None:
                        weather_json = {}
                    
                    for key, rule in SQL_RULES.items():
                        if rule.get("source") == TABLE_SOURCE_WEATHER:
                            col = rule.get("column")
                            json_key = rule.get("json_key")
                            
                            val = None
                            if col == "weather_json" and json_key:
                                val = weather_json.get(json_key)
                                if isinstance(val, list) and len(val) == 24:
                                    for h in range(24):
                                        hourly_data_lists[h][key] = val[h]
                                    continue
                            elif col in row_dict:
                                val = row_dict[col]
                            
                            daily_weather_data[key] = val

        # 3. 聚合与计算
        result_list = []
        for h in range(24):
            lists = hourly_data_lists[h]
            row = {"hour": h}
            
            # 均值聚合
            for key, rule in SQL_RULES.items():
                if rule.get("source") == TABLE_SOURCE_POWER:
                    vals = lists.get(key, [])
                    if vals:
                        row[key] = sum(vals) / len(vals)
                elif key in lists:
                    row[key] = lists[key]
            
            # 填充单日天气
            for k, v in daily_weather_data.items():
                row[k] = v
            
            # 计算衍生字段
            if "price_da" in row and "price_rt" in row:
                row["price_diff"] = row["price_da"] - row["price_rt"]
            
            if "load_forecast" in row and "load_actual" in row:
                row["load_deviation"] = row["load_forecast"] - row["load_actual"]
            
            if "new_energy_forecast" not in row:
                pv = row.get("ne_pv_forecast", 0) or 0
                wind = row.get("ne_wind_forecast", 0) or 0
                if pv > 0 or wind > 0:
                    row["new_energy_forecast"] = pv + wind
            
            result_list.append(row)
            
        return result_list
    except Exception as e:
        print(f"Calculation error for {date}: {e}")
        return None

@app.post("/daily-averages")
async def query_daily_averages(
    dates: str = Form(..., description="日期列表，JSON格式，例如: [\"2023-09-18\", \"2023-09-19\"]"),
    data_type_keyword: str = Form("日前节点电价", description="数据类型关键字"),
    station_name: str = Form(None, description="站点名称（可选）")
):
    """
    查询多天的均值数据
    
    参数:
    - dates: 日期列表，JSON格式
    - data_type_keyword: 数据类型关键字
    - station_name: 站点名称（可选）
    
    返回:
    - 查询结果
    """
    try:
        import json
        date_list = json.loads(dates)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"日期格式错误: {str(e)}")
    
    result = importer.query_daily_averages(date_list, data_type_keyword, station_name)
    
    if result["total"] == 0:
        return {"total": 0, "data": []}
    
    return result

@app.get("/daily-averages/export")
async def export_daily_averages(
    dates: str = Query(..., description="日期列表，JSON格式"),
    data_type_keyword: str = Query("日前节点电价", description="数据类型关键字")
):
    """
    导出多天的均值数据为Excel文件
    
    参数:
    - dates: 日期列表，JSON格式
    - data_type_keyword: 数据类型关键字
    
    返回:
    - Excel文件下载
    """
    try:
        date_list = json.loads(dates)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"日期格式错误: {str(e)}")
    
    # 查询数据
    result = importer.query_daily_averages(date_list, data_type_keyword)
    
    # 生成文件名：多天均值查询_时间戳.xlsx
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"多天均值查询_{timestamp}.xlsx"
    
    if not result["data"]:
        # 如果没有数据，返回空Excel
        df = pd.DataFrame()
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        output.seek(0)
        
        from fastapi.responses import StreamingResponse
        import urllib.parse
        encoded_filename = urllib.parse.quote(filename)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename*=UTF-8''{encoded_filename}"}
        )
    
    # 转换为DataFrame
    df = pd.DataFrame(result["data"])
    
    # 检查是否包含必要的列
    required_columns = ['channel_name', 'record_date', 'record_time', 'value', 'sheet_name']
    if all(col in df.columns for col in required_columns):
        # 类似preHandle.py的处理方式，生成透视表格式
        try:
            # 提取唯一的sheet_name（假设数据中sheet_name唯一）
            sheet_name = df['sheet_name'].unique()[0] if len(df['sheet_name'].unique()) > 0 else 'Sheet1'
            # 提取唯一的日期（假设数据中日期唯一）
            record_date = df['record_date'].unique()[0] if len(df['record_date'].unique()) > 0 else pd.Timestamp.now().date()
            
            # 格式化日期为YYYY-MM-DD
            if hasattr(record_date, 'strftime'):
                record_date_str = record_date.strftime('%Y-%m-%d')
            else:
                record_date_str = str(record_date)
            
            # 处理文件名特殊字符（避免斜杠、空格等导致保存失败）
            sheet_name_clean = str(sheet_name).replace('/', '_').replace('\\', '_').replace(' ', '')
            
            # 转换record_time为小时（处理各种可能的格式）
            def extract_hour(time_value):
                if pd.isna(time_value):
                    return None
                if isinstance(time_value, str):
                    if ':' in time_value:
                        # 格式如 "01:00", "1:00"
                        return int(time_value.split(':')[0])
                    else:
                        # 可能是数字字符串如 "100" 表示 01:00
                        try:
                            time_int = int(time_value)
                            return time_int // 100
                        except:
                            return None
                elif isinstance(time_value, (int, float)):
                    # 数字格式如 100 表示 01:00
                    return int(time_value) // 100
                else:
                    # timedelta或其他格式
                    try:
                        # 如果是timedelta对象
                        hours = time_value.seconds // 3600
                        return hours
                    except:
                        return None
            
            # 应用小时提取函数
            df['hour'] = df['record_time'].apply(extract_hour)
            
            # 删除hour为NaN的行
            df = df.dropna(subset=['hour'])
            
            # 生成电站级透视表
            if len(df) > 0:
                pivot_df = pd.pivot_table(
                    df,
                    index=['channel_name', 'record_date'],
                    columns='hour',
                    values='value',
                    aggfunc='mean'
                )
                
                # 重新索引确保有24小时列，并正确格式化列名
                pivot_df = pivot_df.reindex(columns=range(24), fill_value=np.nan)
                # 确保列名格式为 HH:00
                pivot_df.columns = [f'{int(h):02d}:00' for h in pivot_df.columns]
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
                hour_columns = [f'{h:02d}:00' for h in range(24)]
                # 确保所有小时列都存在
                for col in hour_columns:
                    if col not in pivot_df.columns:
                        pivot_df[col] = np.nan
                
                # 在计算平均值前，确保所有列为数值类型
                for col in hour_columns:
                    pivot_df[col] = pd.to_numeric(pivot_df[col], errors='coerce')
                
                # 计算全省统一均价行
                province_avg = {}
                for col in hour_columns:
                    if col in pivot_df.columns:
                        province_avg[col] = pivot_df[col].mean(skipna=True)
                              
                final_df = pivot_df
            else:
                # 如果处理后没有数据，创建空的DataFrame
                columns = ['节点名称', '日期', '单位'] + [f'{h:02d}:00' for h in range(24)]
                final_df = pd.DataFrame(columns=columns)
            
            # 直接返回Excel文件流
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                final_df.to_excel(writer, index=False, sheet_name=sheet_name_clean[:31])
            output.seek(0)
            
            import urllib.parse
            encoded_filename = urllib.parse.quote(filename)
            from fastapi.responses import StreamingResponse
            return StreamingResponse(
                iter([output.getvalue()]),
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={
                    "Content-Disposition": f"attachment; filename*=UTF-8''{encoded_filename}"
                }
            )
        except Exception as e:
            print(f"处理透视表格式时出错: {e}")
            import traceback
            traceback.print_exc()
    
    # 如果不包含必要列或处理透视表失败，使用原始导出方式
    # 直接返回Excel文件流
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='多天均值数据')
    output.seek(0)
    
    import urllib.parse
    encoded_filename = urllib.parse.quote(filename)
    from fastapi.responses import StreamingResponse
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename*=UTF-8''{encoded_filename}"
        }
    )

@app.post("/daily-averages/export-from-result")
async def export_daily_averages_from_result(
    query_result: str = Form(..., description="查询结果数据"),
    data_type_keyword: str = Form("日前节点电价", description="数据类型关键字")
):
    """
    根据当前查询结果导出多天的均值数据为Excel文件
    
    参数:
    - query_result: 当前查询结果，JSON格式
    - data_type_keyword: 数据类型关键字
    
    返回:
    - Excel文件下载
    """
    try:
        import json
        query_result_data = json.loads(query_result)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"查询结果格式错误: {str(e)}")
    
    # 生成文件名：多天均值查询_时间戳.xlsx
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"多天均值查询_{timestamp}.xlsx"
    
    if not query_result_data:
        # 如果没有数据，返回空Excel
        df = pd.DataFrame()
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        output.seek(0)
        
        from fastapi.responses import StreamingResponse
        import urllib.parse
        encoded_filename = urllib.parse.quote(filename)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename*=UTF-8''{encoded_filename}"}
        )
    
    # 转换为DataFrame
    df = pd.DataFrame(query_result_data)
    
    # 检查是否包含必要的列
    required_columns = ['channel_name', 'record_date', 'record_time', 'value', 'sheet_name']
    if all(col in df.columns for col in required_columns):
        # 类似preHandle.py的处理方式，生成透视表格式
        try:
            # 提取唯一的sheet_name（假设数据中sheet_name唯一）
            sheet_name = df['sheet_name'].unique()[0] if len(df['sheet_name'].unique()) > 0 else 'Sheet1'
            # 提取唯一的日期（假设数据中日期唯一）
            record_date = df['record_date'].unique()[0] if len(df['record_date'].unique()) > 0 else pd.Timestamp.now().date()
            
            # 格式化日期为YYYY-MM-DD
            if hasattr(record_date, 'strftime'):
                record_date_str = record_date.strftime('%Y-%m-%d')
            else:
                record_date_str = str(record_date)
            
            # 处理文件名特殊字符（避免斜杠、空格等导致保存失败）
            sheet_name_clean = str(sheet_name).replace('/', '_').replace('\\', '_').replace(' ', '')
            
            # 转换record_time为小时（处理各种可能的格式）
            # 转换record_time为小时
            def extract_hour(time_value):
                if pd.isna(time_value):
                    return None
                
                try:
                    # 1. 优先处理整数/浮点数
                    if isinstance(time_value, (int, float, np.number)):
                        val = int(time_value)
                        
                        # 【核心修复逻辑】
                        # 如果数值很大（超过2400），说明肯定是秒数，不是HHMM
                        # 例如 3600(秒) / 3600 = 1点
                        if val >= 3600: 
                             return val // 3600
                        
                        # 如果数值在 0-23 之间，直接是小时
                        if 0 <= val < 24:
                            return val
                            
                        # 如果是 HHMM 格式 (例如 100 代表 01:00, 2300 代表 23:00)
                        if 100 <= val <= 2400:
                            return val // 100
                            
                        # 兜底：如果是 0，既可能是0点也可能是0秒，返回0
                        if val == 0:
                            return 0

                    # 2. 处理字符串
                    time_str = str(time_value).strip()
                    if ':' in time_str:
                        return int(time_str.split(':')[0])
                    
                    # 3. 处理 Timedelta 对象
                    if hasattr(time_value, 'total_seconds'):
                        return int(time_value.total_seconds() // 3600)
                    if hasattr(time_value, 'seconds'):
                        return int(time_value.seconds // 3600)

                    # 再次尝试转数字处理（防止字符串类型的数字 "3600"）
                    try:
                        val = int(float(time_str))
                        if val >= 3600: return val // 3600
                        if val < 24: return val
                        return val // 100
                    except:
                        pass

                    return None
                except Exception as e:
                    return None
            
            # 应用小时提取函数
            df['hour'] = df['record_time'].apply(extract_hour)
            print("转换后的前10行数据:")
            print(df[['record_time', 'hour']].head(10))
            print("Hour列的唯一值:", df['hour'].unique())
            # 删除hour为NaN的行
            df = df.dropna(subset=['hour'])
            
            # 生成电站级透视表
            if len(df) > 0:
                pivot_df = pd.pivot_table(
                    df,
                    index=['channel_name', 'record_date'],
                    columns='hour',
                    values='value',
                    aggfunc='mean'
                )
                
                # 重新索引确保有24小时列，并正确格式化列名
                pivot_df = pivot_df.reindex(columns=range(24), fill_value=np.nan)
                # 确保列名格式为 HH:00
                pivot_df.columns = [f'{int(h):02d}:00' for h in pivot_df.columns]
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
                hour_columns = [f'{h:02d}:00' for h in range(24)]
                # 确保所有小时列都存在
                for col in hour_columns:
                    if col not in pivot_df.columns:
                        pivot_df[col] = np.nan
                
                # 在计算平均值前，确保所有列为数值类型
                for col in hour_columns:
                    pivot_df[col] = pd.to_numeric(pivot_df[col], errors='coerce')
                
                # 计算全省统一均价行
                province_avg = {}
                for col in hour_columns:
                    if col in pivot_df.columns:
                        province_avg[col] = pivot_df[col].mean(skipna=True)
                              
                final_df = pivot_df
            else:
                # 如果处理后没有数据，创建空的DataFrame
                columns = ['节点名称', '日期', '单位'] + [f'{h:02d}:00' for h in range(24)]
                final_df = pd.DataFrame(columns=columns)
            
            # 直接返回Excel文件流
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                final_df.to_excel(writer, index=False, sheet_name=sheet_name_clean[:31])
            output.seek(0)
            
            import urllib.parse
            encoded_filename = urllib.parse.quote(filename)
            from fastapi.responses import StreamingResponse
            return StreamingResponse(
                iter([output.getvalue()]),
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={
                    "Content-Disposition": f"attachment; filename*=UTF-8''{encoded_filename}"
                }
            )
        except Exception as e:
            print(f"处理透视表格式时出错: {e}")
            import traceback
            traceback.print_exc()
    
    # 如果不包含必要列或处理透视表失败，使用原始导出方式
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='多天均值数据')
    output.seek(0)
    
    import urllib.parse
    encoded_filename = urllib.parse.quote(filename)
    from fastapi.responses import StreamingResponse
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename*=UTF-8''{encoded_filename}"
        }
    )

@app.post("/price-difference")
async def query_price_difference(
    dates: str = Form(..., description="日期列表，JSON格式，例如: [\"2023-09-18\", \"2023-09-19\"]"),
    region: str = Form("", description="地区前缀，如'云南_'，默认为空"),
    station_name: str = Form(None, description="站点名称（可选）")
):
    """
    查询价差数据（日前节点电价 - 实时节点电价）
    
    参数:
    - dates: 日期列表，JSON格式
    - region: 地区前缀，如"云南_"，默认为空
    - station_name: 站点名称（可选）
    
    返回:
    - 价差查询结果
    """
    try:
        import json
        date_list = json.loads(dates)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"日期格式错误: {str(e)}")
    
    result = importer.query_price_difference(date_list, region, station_name)
    
    return result

@app.post("/price-difference/export-from-result")
async def export_price_difference_from_result(
    query_result: str = Form(..., description="查询结果数据"),
    region: str = Form("", description="地区前缀")
):
    """
    根据当前查询结果导出价差数据为Excel文件
    
    参数:
    - query_result: 当前查询结果，JSON格式
    - region: 地区前缀
    
    返回:
    - Excel文件下载
    """
    try:
        import json
        query_result_data = json.loads(query_result)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"查询结果格式错误: {str(e)}")
    
    # 生成文件名：价差查询_时间戳.xlsx
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"价差查询_{timestamp}.xlsx"
    
    if not query_result_data:
        # 如果没有数据，返回空Excel
        df = pd.DataFrame()
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        output.seek(0)
        
        from fastapi.responses import StreamingResponse
        import urllib.parse
        encoded_filename = urllib.parse.quote(filename)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename*=UTF-8''{encoded_filename}"}
        )
    
    # 转换为DataFrame
    df = pd.DataFrame(query_result_data)
    
    # 检查是否包含必要的列
    required_columns = ['channel_name', 'record_date', 'record_time', 'value', 'sheet_name']
    if all(col in df.columns for col in required_columns):
        # 类似preHandle.py的处理方式，生成透视表格式
        try:
            # 提取唯一的sheet_name
            sheet_name = df['sheet_name'].unique()[0] if len(df['sheet_name'].unique()) > 0 else 'Sheet1'
            record_date = df['record_date'].unique()[0] if len(df['record_date'].unique()) > 0 else pd.Timestamp.now().date()
            
            # 格式化日期为YYYY-MM-DD
            if hasattr(record_date, 'strftime'):
                record_date_str = record_date.strftime('%Y-%m-%d')
            else:
                record_date_str = str(record_date)
            
            # 处理文件名特殊字符
            sheet_name_clean = str(sheet_name).replace('/', '_').replace('\\', '_').replace(' ', '')
            
            # 转换record_time为小时
            def extract_hour(time_value):
                if pd.isna(time_value):
                    return None
                try:
                    if isinstance(time_value, (int, float, np.number)):
                        val = int(time_value)
                        if val >= 3600:
                            return val // 3600
                        if 0 <= val < 24:
                            return val
                        if 100 <= val <= 2400:
                            return val // 100
                        if val == 0:
                            return 0
                    time_str = str(time_value).strip()
                    if ':' in time_str:
                        return int(time_str.split(':')[0])
                    if hasattr(time_value, 'total_seconds'):
                        return int(time_value.total_seconds() // 3600)
                    if hasattr(time_value, 'seconds'):
                        return int(time_value.seconds // 3600)
                    try:
                        val = int(float(time_str))
                        if val >= 3600:
                            return val // 3600
                        if val < 24:
                            return val
                        return val // 100
                    except:
                        pass
                    return None
                except Exception as e:
                    return None
            
            # 应用小时提取函数
            df['hour'] = df['record_time'].apply(extract_hour)
            df = df.dropna(subset=['hour'])
            
            # 生成透视表
            if len(df) > 0:
                pivot_df = pd.pivot_table(
                    df,
                    index=['channel_name', 'record_date'],
                    columns='hour',
                    values='value',
                    aggfunc='mean'
                )
                
                # 重新索引确保有24小时列
                pivot_df = pivot_df.reindex(columns=range(24), fill_value=np.nan)
                pivot_df.columns = [f'{int(h):02d}:00' for h in pivot_df.columns]
                pivot_df = pivot_df.reset_index()
                
                # 修改列名称
                pivot_df = pivot_df.rename(columns={
                    'channel_name': '节点名称',
                    'record_date': '日期'
                })
                
                # 插入单位列
                pivot_df.insert(
                    loc=2,
                    column='单位',
                    value='价差(元/MWh)'
                )
                
                # 确保所有小时列都存在
                hour_columns = [f'{h:02d}:00' for h in range(24)]
                for col in hour_columns:
                    if col not in pivot_df.columns:
                        pivot_df[col] = np.nan
                
                # 确保所有列为数值类型
                for col in hour_columns:
                    pivot_df[col] = pd.to_numeric(pivot_df[col], errors='coerce')
                
                final_df = pivot_df
            else:
                columns = ['节点名称', '日期', '单位'] + [f'{h:02d}:00' for h in range(24)]
                final_df = pd.DataFrame(columns=columns)
            
            # 返回Excel文件流
            output = BytesIO()
            from openpyxl.styles import PatternFill
            # from openpyxl.chart import BarChart, Reference, Series
            
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                final_df.to_excel(writer, index=False, sheet_name=sheet_name_clean[:31])
                
                # 获取工作表
                worksheet = writer.sheets[sheet_name_clean[:31]]
                
                # 应用条件格式：大于0显示绿色渐变，小于0显示红色渐变
                hour_columns = [f'{h:02d}:00' for h in range(24)]
                
                # 找到所有数值中的最大绝对值，用于确定颜色深度
                max_abs_value = 0
                for col in final_df.columns:
                    if col in hour_columns:
                        max_abs_value = max(max_abs_value, final_df[col].abs().max())
                
                # 如果最大绝对值为0，则设为1避免除零错误
                if max_abs_value == 0:
                    max_abs_value = 1
                
                # 定义颜色填充函数
                def get_fill_color(value):
                    if pd.isna(value):
                        return None
                    
                    # 计算颜色强度，基于绝对值比例
                    intensity = abs(value) / max_abs_value
                    
                    # 确保最小亮度，避免颜色过深
                    min_brightness = 150  # 最亮为255
                    brightness_range = 255 - min_brightness
                    brightness = int(min_brightness + (1 - intensity) * brightness_range)
                    
                    if value > 0:
                        # 正数：绿色系，强度越高颜色越深
                        red = brightness
                        green = 255
                        blue = brightness
                    elif value < 0:
                        # 负数：红色系，强度越高颜色越深
                        red = 255
                        green = brightness
                        blue = brightness
                    else:
                        # 零值：白色
                        return None
                    
                    # 转换为十六进制颜色代码
                    color_code = f"{red:02X}{green:02X}{blue:02X}"
                    return PatternFill(start_color=color_code, end_color=color_code, fill_type='solid')
                
                # 找到小时列的列索引并应用条件格式
                for col_idx, col in enumerate(final_df.columns, start=1):
                    if col in hour_columns:
                        # 对每个小时列应用条件格式
                        for row_idx in range(2, len(final_df) + 2):  # 从第2行开始（第1行是表头）
                            cell = worksheet.cell(row=row_idx, column=col_idx)
                            if cell.value is not None:
                                try:
                                    value = float(cell.value)
                                    fill = get_fill_color(value)
                                    if fill:
                                        cell.fill = fill
                                except (ValueError, TypeError):
                                    pass

            output.seek(0)
            
            import urllib.parse
            encoded_filename = urllib.parse.quote(filename)
            from fastapi.responses import StreamingResponse
            return StreamingResponse(
                iter([output.getvalue()]),
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={
                    "Content-Disposition": f"attachment; filename*=UTF-8''{encoded_filename}"
                }
            )
        except Exception as e:
            print(f"处理透视表格式时出错: {e}")
            import traceback
            traceback.print_exc()
    
    # 如果不包含必要列或处理透视表失败，使用原始导出方式
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='价差数据')
    output.seek(0)
    
    import urllib.parse
    encoded_filename = urllib.parse.quote(filename)
    from fastapi.responses import StreamingResponse
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename*=UTF-8''{encoded_filename}"
        }
    )

@app.get("/daily_hourly", response_class=HTMLResponse)
async def daily_hourly_page(request: Request):
    """返回24小时数据展示页面"""
    return templates.TemplateResponse("daily_hourly.html", {"request": request})

@app.get("/similar_day", response_class=HTMLResponse)
async def similar_day_page(request: Request):
    """返回类比日匹配页面"""
    return templates.TemplateResponse("similar_day.html", {"request": request})

@app.get("/api/daily-hourly-data")
async def get_daily_hourly_data(date: str):
    """获取指定日期的24小时数据 (优先查缓存)"""
    try:
        # 1. 尝试从缓存表查询
        table_name = "cache_daily_hourly"
        target_date = pd.to_datetime(date).date()
        date_str = target_date.strftime("%Y-%m-%d")
        
        tables = db_manager.get_tables()
        if table_name in tables:
            with db_manager.engine.connect() as conn:
                # 获取所有列
                sql = text(f"SELECT * FROM {table_name} WHERE record_date = :d ORDER BY hour ASC")
                result = conn.execute(sql, {"d": date_str}).fetchall()
                
                if result:
                    # 转换回字典列表
                    data_list = []
                    for row in result:
                        d = dict(row._mapping)
                        # 处理日期对象转字符串
                        if 'record_date' in d:
                            d['record_date'] = str(d['record_date'])
                        if 'updated_at' in d:
                            d['updated_at'] = str(d['updated_at'])
                        data_list.append(d)
                    return {"status": "success", "data": data_list, "source": "cache"}

        # 2. 如果缓存没命中，实时计算
        print(f"Cache miss for {date_str}, calculating...")
        data = await calculate_daily_hourly_data(date_str)
        
        if data:
            # 3. 异步写入缓存 (简单起见，这里同步写入，或留给下次批量生成)
            # 为了保证下次查询快，最好这里就写入。
            # 但考虑到表可能还没建，或者 calculate_daily_hourly_data 是独立的
            # 我们可以在 calculate_daily_hourly_data 外部再调一次生成逻辑，或者暂时只返回实时数据
            # 既然用户专门要了缓存表，我们应该尽力去存。
            
            # 尝试自动建表并存入? 
            # 简单起见，直接返回实时计算结果，并建议用户点击"生成缓存"
            # 或者，我们可以调用 generate_daily_hourly_cache 的一部分逻辑来存单日
            # 这里我们选择直接返回实时数据，但在前端提示。
            return {"status": "success", "data": data, "source": "realtime"}
        else:
             return {"status": "error", "message": f"未找到 {date} 的电力数据"}

    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/api/generate-price-cache")
async def generate_price_cache(request: Request):
    """
    生成节点电价映射缓存表 -> 合并入 cache_daily_hourly
    """
    try:
        # 1. 获取所有有数据的日期
        all_tables = db_manager.get_tables()
        power_tables = [t for t in all_tables if t.startswith('power_data_')]
        
        dates_to_process = []
        for t in power_tables:
            try:
                d_str = t.replace('power_data_', '')
                dates_to_process.append(d_str) # YYYYMMDD
            except:
                pass
        
        dates_to_process.sort()
        total_days = len(dates_to_process)
        print(f"待处理日期: {total_days} 天")
        
        processed_count = 0
        inserted_count = 0
        
        for date_str in dates_to_process:
            # YYYYMMDD -> YYYY-MM-DD
            target_date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
            
            try:
                count = update_price_cache_for_date(target_date_str)
                inserted_count += count
            except Exception as e:
                print(f"Error processing {date_str}: {e}")
                import traceback
                traceback.print_exc()
                continue
            
            processed_count += 1
            if processed_count % 10 == 0:
                print(f"Price Cache: Processed {processed_count}/{total_days} days")

        return {
            "status": "success", 
            "processed_days": processed_count, 
            "inserted_records": inserted_count,
            "table": "cache_daily_hourly"
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"error": str(e)})

def update_price_cache_for_date(target_date_str: str, only_weather: bool = False) -> int:
    """
    更新指定日期的电价缓存 (供 generate_price_cache 和 import_file 调用)
    返回插入/更新的记录数 (最大24)
    
    Args:
        target_date_str: 目标日期 YYYY-MM-DD
        only_weather: 是否只更新天气数据 (保留原有电力数据)
    """
    from sql_config import SQL_RULES, TABLE_SOURCE_POWER, TABLE_SOURCE_WEATHER
    
    table_name = "cache_daily_hourly"

    # 1. 确保表存在
    # (为了性能，这里可以假设表已存在，或者每次都检查，对于单次导入检查一下无妨)
    # 构建字段列表
    columns_def = [
        "`record_date` DATE NOT NULL",
        "`hour` TINYINT NOT NULL",
        "`updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
    ]
    
    calc_fields = {
        "price_diff": "FLOAT COMMENT '价差'",
        "load_deviation": "FLOAT COMMENT '负荷偏差'",
        "new_energy_forecast": "FLOAT COMMENT '新能源预测总和'"
    }
    
    all_fields = {}
    for key, rule in SQL_RULES.items():
        field_name = key
        if key in ['date', 'day_type', 'week_day', 'weather', 'wind_direction']:
            col_type = "VARCHAR(50)"
        else:
            col_type = "FLOAT"
        all_fields[field_name] = f"`{field_name}` {col_type} COMMENT '{rule.get('name', '')}'"
        
    for k, v in calc_fields.items():
        all_fields[k] = f"`{k}` {v}"
        
    cols_sql = ",\n".join(list(all_fields.values()) + columns_def)
    
    with db_manager.engine.begin() as conn:
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {cols_sql},
            PRIMARY KEY (`record_date`, `hour`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        conn.execute(text(create_sql))
    
    # 2. 获取数据 (使用 sql_config 中的规则动态查询)
    # from sql_config import SQL_RULES, TABLE_SOURCE_POWER, TABLE_SOURCE_WEATHER (Moved to top)
    
    # 2.1 构造小时数据映射 {hour: {field_name: [val1, val2]}}
    # ...
    
    hourly_map = {h: {} for h in range(24)}
    
    # 初始化字段列表 (用于 hourly_map)
    # 包括 price_da, price_rt 以及 SQL_RULES 中定义的所有 POWER 数据
    field_keys = ['price_da', 'price_rt']
    for k, v in SQL_RULES.items():
        if v.get('source') == TABLE_SOURCE_POWER and k not in ['price_da', 'price_rt']:
            field_keys.append(k)
            
    # 如果 only_weather=True，则不需要初始化这些字段的列表，也不需要查询电力数据
    if not only_weather:
        for h in range(24):
            for k in field_keys:
                hourly_map[h][k] = []

        # 2.2 获取日前/实时电价 (保留之前的特定逻辑：区域过滤)
        da_result = importer.query_daily_averages([target_date_str], "日前节点电价")
        da_data = da_result.get("data", [])
        
        rt_result = importer.query_daily_averages([target_date_str], "实时节点电价")
        rt_data = rt_result.get("data", [])
        
        def filter_and_process_price(data_list, type_key):
            filtered = [item for item in data_list if "云南" not in str(item.get('type', ''))]
            has_guangdong = any("广东" in str(item.get('type', '')) for item in filtered)
            if has_guangdong:
                filtered = [item for item in filtered if "广东" in str(item.get('type', ''))]
            
            for item in filtered:
                rt_val = item['record_time']
                norm_time = normalize_record_time(rt_val, target_date_str)
                if norm_time is None:
                    continue
                
                hour = norm_time.hour
                if 0 <= hour <= 23:
                    val = float(item['value']) if item['value'] is not None else 0
                    hourly_map[hour][type_key].append(val)

        filter_and_process_price(da_data, 'price_da')
        filter_and_process_price(rt_data, 'price_rt')

        # 2.3 获取 SQL_RULES 中定义的其他电力数据
        # 构造表名
        d_obj = datetime.datetime.strptime(target_date_str, "%Y-%m-%d")
        table_name_power = f"power_data_{d_obj.strftime('%Y%m%d')}"
        
        # 检查表是否存在
        if table_name_power in db_manager.get_tables():
            with db_manager.engine.connect() as conn:
                for key, rule in SQL_RULES.items():
                    if rule.get('source') == TABLE_SOURCE_POWER and key not in ['price_da', 'price_rt']:
                        where_clause = rule.get('where')
                        if not where_clause:
                            continue
                            
                        try:
                            sql = text(f"SELECT record_time, value FROM {table_name_power} WHERE {where_clause}")
                            result = conn.execute(sql).fetchall()
                            
                            for row in result:
                                rt_val = row[0]
                                norm_time = normalize_record_time(rt_val, target_date_str)
                                if norm_time is None:
                                    continue
                                
                                hour = norm_time.hour
                                if 0 <= hour <= 23:
                                    val = float(row[1]) if row[1] is not None else 0
                                    hourly_map[hour][key].append(val)
                        except Exception as e:
                            print(f"查询规则 {key} 失败: {e}")

    # 2.4 获取 SQL_RULES 中定义的天气数据 (TABLE_SOURCE_WEATHER)
    # 这部分数据需要从 calendar_weather 表中查询，然后拆解 json
    # 查询该日期的天气数据
    weather_row = None
    with db_manager.engine.connect() as conn:
        try:
            sql = text("SELECT * FROM calendar_weather WHERE date = :d")
            weather_row = conn.execute(sql, {"d": target_date_str}).mappings().fetchone()
        except Exception as e:
            print(f"查询天气数据失败: {e}")

    # 无论是否有 weather_row，如果该日期只有天气数据而没有电力数据，我们也希望能入库
    # 所以必须确保遍历到所有可能的来源
    
    if weather_row:
        # 解析 JSON
        weather_json = None
        if weather_row.get('weather_json'):
            try:
                if isinstance(weather_row['weather_json'], str):
                    weather_json = json.loads(weather_row['weather_json'])
                else:
                    weather_json = weather_row['weather_json']
            except:
                pass
        
        # 遍历规则填充数据
        for key, rule in SQL_RULES.items():
            if rule.get('source') == TABLE_SOURCE_WEATHER:
                # 1. 直接映射列
                col_name = rule.get('column')
                json_key = rule.get('json_key')
                
                # 如果有 json_key，则从 JSON 中取值 (通常是数组)
                if json_key and weather_json and json_key in weather_json:
                    values = weather_json[json_key]
                    if isinstance(values, list):
                        # 假设数组长度为 24，对应 0-23 小时
                        # 如果不足 24，则尽力填充
                        for h in range(min(len(values), 24)):
                            val = values[h]
                            if val is not None:
                                try:
                                    hourly_map[h].setdefault(key, []).append(float(val))
                                except (ValueError, TypeError):
                                    hourly_map[h].setdefault(key, []).append(val)
                
                # 2. 如果没有 json_key，则是取列的标量值 (全天相同)
                elif col_name and col_name in weather_row and not json_key:
                    val = weather_row[col_name]
                    # 特殊处理日期字段，将其转换为字符串
                    if isinstance(val, (datetime.date, datetime.datetime)):
                        val = val.strftime("%Y-%m-%d")
                        
                    if val is not None:
                        # 全天 24 小时都用这个值
                        for h in range(24):
                            # 注意：如果是字符串，append 后求均值会报错
                            # 这里需要判断类型
                            if isinstance(val, (int, float)):
                                hourly_map[h].setdefault(key, []).append(float(val))
                            else:
                                hourly_map[h].setdefault(key, []).append(val)
    
    # 即使没有 weather_row，也可能因为有电力数据而继续执行
    # 如果只有天气数据没有电力数据，也会因为 weather_row 存在而有数据
    # 如果两者都没有，下面的 batch_data 为空，返回 0

    # 4. 构造入库数据
    batch_data = []
    
    # 收集所有需要更新的字段
    all_update_fields = set()
    
    if not only_weather:
        all_update_fields.add('price_da')
        all_update_fields.add('price_rt')
        all_update_fields.add('price_diff')
        all_update_fields.add('new_energy_forecast')
        all_update_fields.add('load_deviation')
        for k in field_keys:
            all_update_fields.add(k)
    
    # 添加天气相关字段到更新列表
    for key, rule in SQL_RULES.items():
        if rule.get('source') == TABLE_SOURCE_WEATHER:
            all_update_fields.add(key)

    for h in range(24):
        row_data = {
            "record_date": target_date_str,
            "hour": h
        }
        
        has_data = False
        
        # 处理均值字段
        for k in list(all_update_fields): # 遍历所有可能字段
            if k in ['record_date', 'hour', 'price_diff', 'new_energy_forecast', 'load_deviation']:
                continue
                
            vals = hourly_map[h].get(k, [])
            if vals:
                # 检查是否是数字
                first_val = vals[0]
                # 特殊处理：如果 first_val 是 datetime.date 对象，也转为字符串
                if isinstance(first_val, (datetime.date, datetime.datetime)):
                    first_val = first_val.strftime("%Y-%m-%d")
                    row_data[k] = first_val
                elif isinstance(first_val, (int, float)):
                    avg = sum(vals) / len(vals)
                    row_data[k] = avg
                else:
                    # 非数字，取第一个非空值
                    row_data[k] = first_val
                has_data = True
            else:
                row_data[k] = None
                
            # [新增] 对所有 row_data 的值再次进行类型清洗，确保没有 date 对象
            val = row_data[k]
            if isinstance(val, (datetime.date, datetime.datetime)):
                row_data[k] = val.strftime("%Y-%m-%d")
        
        # 如果整行没有任何数据(连电价都没有)，是否跳过？
        # 如果是增量更新，可能只想更新部分字段。
        # 但如果是 Upsert，None 会覆盖旧值吗？
        # 我们应该只包含有值的字段，或者全部包含。
        # 这里选择：如果没有任何数据，跳过该小时；否则插入/更新所有字段。
        # 修改逻辑：只要有天气数据也算有数据，不能跳过
        if not has_data:
            continue
            
        # 计算衍生字段
        # 1. 价差
        p_da = row_data.get('price_da')
        p_rt = row_data.get('price_rt')
        # 修改逻辑：只要其中一个有值就可以更新，而不是必须两个都有
        # 如果只有一个有值，diff 为 None (因为无法计算价差)，但原有的值应该保留
        if p_da is not None and p_rt is not None:
            row_data['price_diff'] = p_da - p_rt
        else:
            row_data['price_diff'] = None
            
        # 2. 新能源预测总和 (光伏+风电)
        # 假设规则里有 ne_pv_forecast 和 ne_wind_forecast
        pv = row_data.get('ne_pv_forecast', 0) or 0
        wind = row_data.get('ne_wind_forecast', 0) or 0
        if pv or wind:
            row_data['new_energy_forecast'] = pv + wind
        else:
            row_data['new_energy_forecast'] = None

        # 3. 负荷偏差 (预测 - 实际)
        l_fore = row_data.get('load_forecast')
        l_act = row_data.get('load_actual')
        if l_fore is not None and l_act is not None:
            row_data['load_deviation'] = l_fore - l_act
        else:
            row_data['load_deviation'] = None
            
        # [新增] 确保 record_date 和 hour 始终存在 (虽然前面已经定义了)
        row_data['record_date'] = target_date_str
        row_data['hour'] = h
            
        batch_data.append(row_data)
    
    # 5. 入库
    if batch_data:
        # 动态构建 SQL
        # 字段列表: record_date, hour + 其他所有字段
        # 因为 batch_data 里的 keys 可能不完全一致(有些是 None)，最好统一一下
        # 其实 executemany 要求所有字典 keys 一致
        
        # 确保所有字典都有所有字段
        final_keys = list(all_update_fields)
        # 过滤掉不在 batch_data[0] 里的 key (虽然我们在循环里都加了)
        # 为了安全，重新整理 batch_data
        
        # 移除 'record_date' 和 'hour'，因为它们已经单独处理
        if 'record_date' in final_keys:
             final_keys.remove('record_date')
        if 'hour' in final_keys:
             final_keys.remove('hour')
             
        # [DEBUG] 打印一下 final_keys 和 batch_data 的样例，方便调试
        if len(batch_data) > 0:
             print(f"[DEBUG] Cache Update for {target_date_str}: {len(batch_data)} records")
             # print(f"[DEBUG] Keys: {final_keys}")
             # print(f"[DEBUG] Sample Row: {batch_data[0]}")
        else:
             print(f"[DEBUG] Cache Update for {target_date_str}: NO DATA to update.")
             if weather_row:
                 print(f"[DEBUG] Weather Row found but no data mapped? Weather Keys: {weather_row.keys()}")
             else:
                 print(f"[DEBUG] No Weather Row and No Power Data.")
        
        clean_batch = []
        for row in batch_data:
            clean_row = {"record_date": row["record_date"], "hour": row["hour"]}
            for k in final_keys:
                clean_row[k] = row.get(k) # 默认为 None
            clean_batch.append(clean_row)
            
        # 构建 INSERT ... ON DUPLICATE KEY UPDATE 语句
        field_list = [f"`{k}`" for k in final_keys]
        param_list = [f":{k}" for k in final_keys]
        
        # UPDATE 部分
        update_parts = [f"`{k}`=VALUES(`{k}`)" for k in final_keys]
        
        # 注意: 这里的 record_date 和 hour 需要显式加入 VALUES 列表，但不在 UPDATE 列表(主键)
        sql = f"""
            INSERT INTO {table_name} 
            (`record_date`, `hour`, {', '.join(field_list)})
            VALUES (:record_date, :hour, {', '.join(param_list)})
            ON DUPLICATE KEY UPDATE
            {', '.join(update_parts)}
        """
        
        with db_manager.engine.begin() as conn:
             try:
                conn.execute(text(sql), clean_batch)
             except Exception as e:
                 print(f"⚠️ SQL Execution Failed for {target_date_str}: {e}")
                 import traceback
                 traceback.print_exc()
                 raise e # 重新抛出以便上层捕获
            
        return len(clean_batch)
    
    return 0

def normalize_record_time(val, date_str):
    """标准化时间字段，处理 timedelta 和 datetime"""
    try:
        # 1. 已经是 datetime
        if isinstance(val, datetime.datetime):
            return val
            
        # 2. 是 timedelta (Python/Pandas/NumPy)
        # 注意: pd.Timedelta 也是 timedelta 的子类 (在某些版本中)，或者行为类似
        # 分开检查更稳妥
        is_delta = isinstance(val, (datetime.timedelta, pd.Timedelta, np.timedelta64))
        
        if is_delta:
            base_date = pd.to_datetime(date_str)
            return base_date + val
            
        # 3. 尝试 pd.to_datetime (针对字符串或 timestamp)
        # 如果 val 是 timedelta 类型的字符串 (如 "00:15:00")，pd.to_datetime 可能会报错或行为不符合预期
        # 所以先尝试转 timedelta
        try:
            base_date = pd.to_datetime(date_str)
            delta = pd.to_timedelta(val)
            return base_date + delta
        except:
            pass

        return pd.to_datetime(val)
    except:
        # 4. 最后的尝试
        try:
            base_date = pd.to_datetime(date_str)
            # 假设 val 是某种可以转为 timedelta 的东西
            delta = pd.to_timedelta(val)
            return base_date + delta
        except:
            # 打印错误以便调试，但在生产环境中可能太吵
            # print(f"Failed to normalize time: {val} type: {type(val)}")
            return None

if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)