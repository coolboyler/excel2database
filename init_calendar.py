import sys
import os
import datetime

# 确保当前目录在路径中，以便导入 modules
sys.path.append(os.getcwd())

import calendar_weather

# 配置日期范围
START_DATE = datetime.date(2023, 1, 1)
END_DATE = datetime.date(2027, 12, 31)

if __name__ == "__main__":
    calendar_weather.update_calendar(START_DATE, END_DATE)
