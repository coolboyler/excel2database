import os
from pred_reader import PowerDataImporter

# 创建导入器实例
importer = PowerDataImporter()

# 指定要测试的文件路径
excel_file = "data/实时节点电价查询(2025-09-18).xlsx"

# 指定要测试的方法
# 这里可以是 importer.import_power_data / importer.import_custom_excel / importer.import_custom_excel_pred
method = importer.import_point_data

print(f"📥 测试导入: {os.path.basename(excel_file)} 使用方法 {method.__name__}")

# 执行导入
success = method(excel_file)

if success:
    print(f"✅ 测试导入成功！")
else:
    print(f"❌ 测试导入失败！")
