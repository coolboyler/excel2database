import pandas as pd
import os
import glob
from pred_reader import PowerDataImporter

def main():
    importer = PowerDataImporter()
    
    print("🚀 启动程序...")
    
    data_folder = "data"
    excel_files = glob.glob(os.path.join(data_folder, "*.xlsx"))
    
    if not excel_files:
        print(f"❌ 在 {data_folder} 文件夹中未找到任何Excel文件")
        return
    
    excel_files.sort()
    print(f"📁 找到 {len(excel_files)} 个Excel文件:")
    for i, file in enumerate(excel_files, 1):
        print(f"  {i}. {os.path.basename(file)}")
    
    success_count = 0
    for excel_file in excel_files:
        print(f"\n{'='*50}")
        print(f"📥 开始导入: {os.path.basename(excel_file)}")
        print(f"{'='*50}")
        
        file_name = os.path.basename(excel_file)
        
        # 自动选择导入方法
        if "负荷实际信息" in file_name or "负荷预测信息" in file_name:
            method = importer.import_power_data
        elif "信息披露(区域)查询实际信息" in file_name:
            method = importer.import_custom_excel
        elif "信息披露(区域)查询预测信息" in file_name:
            method = importer.import_custom_excel_pred
        elif "实时节点电价查询" in file_name or "日前节点电价查询" in file_name:
            method = importer.import_point_data
        else:
            # 使用新的导入方法处理未知格式的Excel文件
            method = importer.import_and_create_new_table
            print(f"⚠️ 使用通用导入方法处理: {file_name}")
        
        # 执行导入
        result = method(excel_file)
        if isinstance(result, tuple) and len(result) == 4:
            success, table_name, record_count, preview_data = result
        else:
            success = result
            table_name = "unknown"
            record_count = 0
            preview_data = []
            
        if success:
            print(f"✅ {file_name} 导入完成！表名: {table_name}, 记录数: {record_count}")
            success_count += 1
        else:
            print(f"❌ {file_name} 导入失败！")
    
    print(f"\n🎉 处理完成！成功: {success_count}/{len(excel_files)} 个文件")
    
    # 示例：演示联表查询功能
    print(f"\n{'='*50}")
    print("🔍 联表查询功能演示")
    print(f"{'='*50}")
    
    # 获取所有表
    from database import DatabaseManager
    db_manager = DatabaseManager()
    tables = db_manager.get_tables()
    
    if len(tables) >= 2:
        print(f"📋 数据库中的表: {tables[:3]}{'...' if len(tables) > 3 else ''}")
        
        # 执行简单的联表查询示例（假设前两个表有相同结构）
        table_names = tables[:2]
        print(f"🔄 对前两个表进行联表查询: {table_names}")
        
        join_result = importer.execute_join_query(
            table_names=table_names,
            select_fields=f"{table_names[0]}.channel_name, {table_names[0]}.value as value1, {table_names[1]}.value as value2",
            limit=10
        )
        
        if join_result["total"] > 0:
            print(f"✅ 联表查询成功，共找到 {join_result['total']} 条记录")
            print("📊 查询结果示例:")
            for i, row in enumerate(join_result["data"][:3]):
                print(f"  {i+1}. {row}")
        else:
            print("⚠️ 联表查询未返回结果")
    else:
        print("ℹ️  数据库中表数量不足，无法演示联表查询")

if __name__ == "__main__":
    main()