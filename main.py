import pandas as pd
import os
import glob
from pred_reader import PowerDataImporter

# =========================
# 主函数
# =========================
def main():
    importer = PowerDataImporter()
    
    print("🚀 启动程序...")
    
    # 自动读取data文件夹中的所有Excel文件
    data_folder = "data"
    excel_files = glob.glob(os.path.join(data_folder, "*.xlsx"))
    
    if not excel_files:
        print(f"❌ 在 {data_folder} 文件夹中未找到任何Excel文件")
        return
    
    # 按文件名排序
    excel_files.sort()
    
    print(f"📁 找到 {len(excel_files)} 个Excel文件:")
    for i, file in enumerate(excel_files, 1):
        print(f"  {i}. {os.path.basename(file)}")
    
    # 交互式选择
    print("\n📋 请选择要执行的操作：")
    print("1. 导入所有文件")
    print("2. 选择特定文件")
    
    choice = input("请输入选择 (1-2): ").strip()
    
    files_to_process = []
    
    if choice == "1":
        files_to_process = excel_files
        print("🔄 开始导入所有文件...")
    elif choice == "2":
        print("🔢 请输入要导入的文件编号（多个用空格分隔）:")
        file_numbers = input("文件编号: ").strip().split()
        
        for num in file_numbers:
            try:
                index = int(num) - 1
                if 0 <= index < len(excel_files):
                    files_to_process.append(excel_files[index])
                else:
                    print(f"⚠️ 跳过无效编号: {num}")
            except ValueError:
                print(f"⚠️ 跳过无效输入: {num}")
    else:
        print("❌ 无效选择，程序退出")
        return
    
    if not files_to_process:
        print("❌ 没有选择任何文件")
        return
    
    # 执行导入
    success_count = 0
    for excel_file in files_to_process:
        print(f"\n{'='*50}")
        print(f"📥 开始导入: {os.path.basename(excel_file)}")
        print(f"{'='*50}")
        
        success = importer.import_power_data(excel_file=excel_file)
        
        if success:
            print(f"✅ {os.path.basename(excel_file)} 导入完成！")
            success_count += 1
        else:
            print(f"❌ {os.path.basename(excel_file)} 导入失败！")

    print(f"\n🎉 处理完成！成功: {success_count}/{len(files_to_process)} 个文件")

if __name__ == "__main__":
    main()