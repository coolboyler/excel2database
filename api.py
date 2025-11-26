# api.py

from io import BytesIO
import json
import time
from fastapi import FastAPI, Query, UploadFile, File, Form, HTTPException, BackgroundTasks, Request
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
    excel_files.sort()
    
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
@app.post("/import")
async def import_file(filename: str = Form(...)):
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
    elif "信息披露(区域)查询实际信息" in filename:
        method = importer.import_custom_excel
    elif "信息披露(区域)查询预测信息" in filename:
        method = importer.import_custom_excel_pred
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
    
    # import_custom_excel 返回两个结果元组，其他方法返回单个四元组
    if method == importer.import_custom_excel:
        # 解包三个结果元组
        (success1, table_name1, record_count1, preview_data1), (success2, table_name2, record_count2, preview_data2),(success3,table_name3,record_count3,preview_data3) = result
        # 合并结果，这里我们使用三个结果的组合
        success = success1 and success2 and success3
        table_name = f"{table_name1}, {table_name2}, {table_name3}"
        record_count = record_count1 + record_count2 + record_count3
        preview_data = preview_data1 + preview_data2 + preview_data3
    elif method == importer.import_custom_excel_pred:
        (success1, table_name1, record_count1, preview_data1), (success2, table_name2, record_count2, preview_data2), (success4, table_name4, record_count4, preview_data4), (success5, table_nam5, record_count5, preview_data5) = result
        # 合并结果，这里我们使用三个结果的组合
        success = success1 and success2 and success4 and success5
        table_name = f"{table_name1}, {table_name2}, {table_name4}, {table_nam5}"
        record_count = record_count1 + record_count2 + record_count4 + record_count5 
        preview_data = preview_data1 + preview_data2 + preview_data4 + preview_data5 
    else:
        # 其他导入方法的常规处理
        success, table_name, record_count, preview_data = result
        
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
            data_query = f"SELECT * FROM {table_name} {where_clause} LIMIT :limit OFFSET :offset"
            params.update({"limit": limit, "offset": offset})
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

@app.post("/daily-averages")
async def query_daily_averages(
    dates: str = Form(..., description="日期列表，JSON格式，例如: [\"2023-09-18\", \"2023-09-19\"]"),
    data_type_keyword: str = Form("日前节点电价", description="数据类型关键字")
):
    """
    查询多天的均值数据
    
    参数:
    - dates: 日期列表，JSON格式
    - data_type_keyword: 数据类型关键字
    
    返回:
    - 查询结果
    """
    try:
        import json
        date_list = json.loads(dates)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"日期格式错误: {str(e)}")
    
    result = importer.query_daily_averages(date_list, data_type_keyword)
    
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

if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=8003, reload=True)