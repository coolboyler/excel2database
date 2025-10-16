from fastapi import FastAPI, UploadFile, File, Form, HTTPException, BackgroundTasks, Request
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import os
import glob
import shutil
from typing import List, Optional
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

@app.post("/import")
async def import_file(filename: str = Form(...)):
    """导入指定的Excel文件到数据库"""
    data_folder = "data"
    file_path = os.path.join(data_folder, filename)
    
    # 检查文件是否存在
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"文件 {filename} 不存在")
    
    # 自动选择导入方法
    if "负荷实际信息" in filename or "负荷预测信息" in filename:
        method = importer.import_power_data
    elif "信息披露(区域)查询实际信息" in filename:
        method = importer.import_custom_excel
    elif "信息披露(区域)查询预测信息" in filename:
        method = importer.import_custom_excel_pred
    elif "实时节点电价查询" in filename or "日前节点电价查询" in filename:
        method = importer.import_point_data
    else:
        raise HTTPException(status_code=400, detail=f"无匹配的导入规则: {filename}")
    
    # 执行同步导入
    success, table_name, record_count, preview_data = method(file_path)
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

@app.delete("/tables/{table_name}")
async def delete_table(table_name: str):
    """删除指定表"""
    success = db_manager.delete_table(table_name)
    if success:
        return {"status": "success", "message": f"表 {table_name} 已删除"}
    else:
        raise HTTPException(status_code=500, detail=f"删除表 {table_name} 失败")
async def import_all_files(background_tasks: BackgroundTasks):
    """导入data目录中的所有Excel文件"""
    data_folder = "data"
    excel_files = glob.glob(os.path.join(data_folder, "*.xlsx"))
    
    if not excel_files:
        raise HTTPException(status_code=404, detail=f"在 {data_folder} 文件夹中未找到任何Excel文件")
    
    # 添加所有文件到后台任务
    for excel_file in excel_files:
        filename = os.path.basename(excel_file)
        background_tasks.add_task(import_file, filename, None)
    
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

if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)