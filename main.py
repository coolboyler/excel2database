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
        elif "实时节点电价查询" or "日前节点电价查询" in file_name:
            method = importer.import_point_data
        else:
            print(f"⚠️ 无匹配的导入规则，跳过: {file_name}")
            continue
        
        success = method(excel_file)
        if success:
            print(f"✅ {file_name} 导入完成！")
            success_count += 1
        else:
            print(f"❌ {file_name} 导入失败！")
    
    print(f"\n🎉 处理完成！成功: {success_count}/{len(excel_files)} 个文件")

if __name__ == "__main__":
    main()
