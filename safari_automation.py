import time
import os
import shutil
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

class CompleteDataDownloader:
    def __init__(self, base_download_dir="~/Downloads/2025_power_data"):
        self.driver = webdriver.Safari()
        self.wait = WebDriverWait(self.driver, 15)
        self.base_download_dir = os.path.expanduser(base_download_dir)
        self.safari_download_dir = os.path.expanduser("~/Downloads")
        
        # 创建目录结构
        self.ensure_directories()
        
        # 下载统计
        self.download_count = 0
        self.success_count = 0
        self.failed_count = 0
        
        # 记录已处理的文件，避免重复
        self.processed_files = set()
    
    def ensure_directories(self):
        """确保所有必要的目录都存在"""
        if not os.path.exists(self.base_download_dir):
            os.makedirs(self.base_download_dir)
            print(f"✓ 创建主目录: {self.base_download_dir}")
    
    def navigate_to_target_page(self):
        """导航到目标页面（只需要运行一次）"""
        try:
            print("开始导航到目标页面...")
            
            # 步骤1: 点击"我的交易"
            print("步骤1: 点击'我的交易'")
            my_trade = self.wait.until(
                EC.element_to_be_clickable((By.XPATH, "//li[contains(@class, 'active')]//*[contains(text(), '我的交易')]"))
            )
            my_trade.click()
            print("✓ '我的交易'点击成功")
            time.sleep(2)
            
            # 步骤2: 点击"实时交易"
            print("步骤2: 点击'实时交易'")
            realtime_trade = self.wait.until(
                EC.element_to_be_clickable((By.XPATH, "//li[contains(@class, 'leftmenu-item')]//*[contains(text(), '实时交易')]"))
            )
            realtime_trade.click()
            print("✓ '实时交易'点击成功")
            time.sleep(2)
            
            # 步骤3: 点击"实时节点电价查询"
            print("步骤3: 点击'实时节点电价查询'")
            price_query = self.wait.until(
                EC.element_to_be_clickable((By.XPATH, "//span[contains(@class, 'title-text') and contains(text(), '实时节点电价查询')]"))
            )
            price_query.click()
            print("✓ '实时节点电价查询'点击成功")
            time.sleep(1)  # 等待1秒
            
            print("🎉 导航完成，已进入目标页面")
            return True
            
        except Exception as e:
            print(f"❌ 导航到目标页面失败: {e}")
            return False
    
    def get_latest_xlsx_file(self):
        """获取Downloads文件夹中最新的xlsx文件"""
        try:
            xlsx_files = [f for f in os.listdir(self.safari_download_dir) 
                         if f.endswith('.xlsx') and not f.startswith('.')]
            
            if not xlsx_files:
                return None
            
            latest_file = max(
                [os.path.join(self.safari_download_dir, f) for f in xlsx_files],
                key=os.path.getctime
            )
            
            return latest_file
        except Exception as e:
            print(f"获取最新xlsx文件失败: {e}")
            return None
    
    def wait_for_xlsx_download(self, timeout=45, check_interval=2):
        """等待xlsx文件下载完成"""
        print("等待xlsx文件下载完成...")
        
        # 记录初始文件状态
        initial_files = set([f for f in os.listdir(self.safari_download_dir) 
                           if f.endswith('.xlsx') and not f.startswith('.')])
        
        print(f"初始xlsx文件数量: {len(initial_files)}")
        
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            current_files = set([f for f in os.listdir(self.safari_download_dir) 
                               if f.endswith('.xlsx') and not f.startswith('.')])
            
            new_files = current_files - initial_files
            
            if new_files:
                # 找到最新的新文件
                new_file = max(
                    [os.path.join(self.safari_download_dir, f) for f in new_files],
                    key=os.path.getctime
                )
                
                print(f"检测到新xlsx文件: {os.path.basename(new_file)}")
                
                # 等待文件完全下载（xlsx文件可能较大）
                return self.wait_for_file_complete(new_file)
            
            time.sleep(check_interval)
        
        print("❌ xlsx文件下载超时")
        return None
    
    def wait_for_file_complete(self, file_path, max_checks=15):
        """等待文件完全下载完成"""
        print(f"检查文件是否下载完成: {os.path.basename(file_path)}")
        
        file_size = -1
        stable_count = 0
        
        for i in range(max_checks):
            try:
                current_size = os.path.getsize(file_path)
                
                if current_size == file_size and current_size > 0:
                    stable_count += 1
                    if stable_count >= 2:  # 连续2次大小不变认为下载完成
                        print(f"✓ 文件下载完成: {os.path.basename(file_path)} (大小: {current_size} 字节)")
                        return file_path
                else:
                    stable_count = 0
                    file_size = current_size
                    print(f"文件大小变化: {current_size} 字节")
                
            except OSError as e:
                print(f"检查文件大小时出错: {e}")
            
            time.sleep(1)
        
        print("❌ 文件下载未在预期时间内完成")
        return None
    
    def move_xlsx_file(self, source_path, date_info):
        """移动并重命名xlsx文件"""
        try:
            # 创建有意义的文件名
            if date_info:
                new_filename = f"{date_info}.xlsx"
            else:
                timestamp = time.strftime("%Y%m%d_%H%M%S")
                new_filename = f"data_{timestamp}.xlsx"
            
            new_filepath = os.path.join(self.base_download_dir, new_filename)
            
            # 确保目标文件不存在
            if os.path.exists(new_filepath):
                print(f"⚠️ 目标文件已存在，创建新名称: {new_filename}")
                counter = 1
                while os.path.exists(new_filepath):
                    name, ext = os.path.splitext(new_filename)
                    new_filename = f"{name}_{counter}{ext}"
                    new_filepath = os.path.join(self.base_download_dir, new_filename)
                    counter += 1
            
            # 移动文件
            shutil.move(source_path, new_filepath)
            print(f"✓ xlsx文件已保存为: {new_filename}")
            
            # 记录已处理的文件
            self.processed_files.add(new_filepath)
            
            return new_filepath
            
        except Exception as e:
            print(f"❌ 移动文件失败: {e}")
            return None
    
    def click_export_and_save(self, date_info=""):
        """点击导出并保存xlsx文件"""
        try:
            print(f"步骤: 点击导出 {date_info}")
            
            # 点击导出按钮
            export_button = self.wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'el-button')]//span[text()='导出']"))
            )
            export_button.click()
            print("✓ 导出按钮点击成功")
            
            # 等待xlsx文件下载完成
            downloaded_file = self.wait_for_xlsx_download()
            
            if downloaded_file:
                # 移动并重命名文件
                saved_filepath = self.move_xlsx_file(downloaded_file, date_info)
                
                if saved_filepath:
                    self.success_count += 1
                    return saved_filepath
                else:
                    print("❌ 文件保存失败")
                    self.failed_count += 1
                    return None
            else:
                print("❌ xlsx文件下载失败")
                self.failed_count += 1
                return None
                
        except Exception as e:
            print(f"❌ 导出失败: {e}")
            self.failed_count += 1
            return None
    
    def select_year(self, target_year="2025"):
        """选择特定年份"""
        try:
            print(f"步骤: 选择年份 {target_year}")
            
            date_input = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "input.el-input__inner[placeholder*='日期']"))
            )
            date_input.click()
            print("✓ 日期输入框点击成功")
            time.sleep(2)
            
            year_header = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".el-date-picker__header-label"))
            )
            year_header.click()
            print("✓ 年份选择按钮点击成功")
            time.sleep(2)
            
            year_cell = self.wait.until(
                EC.element_to_be_clickable((By.XPATH, f"//a[contains(@class, 'cell') and text()='{target_year}']"))
            )
            year_cell.click()
            print(f"✓ 年份 {target_year} 选择成功")
            time.sleep(2)
            
            return True
            
        except Exception as e:
            print(f"❌ 选择年份失败: {e}")
            return False
    
    def select_month(self, month_name):
        """选择特定月份"""
        try:
            print(f"步骤: 选择月份 {month_name}")
            
            month_cell = self.wait.until(
                EC.element_to_be_clickable((By.XPATH, f"//a[contains(@class, 'cell') and text()='{month_name}']"))
            )
            month_cell.click()
            print(f"✓ 月份 {month_name} 选择成功")
            time.sleep(2)
            
            return True
            
        except Exception as e:
            print(f"❌ 选择月份失败: {e}")
            return False
    
    def select_day(self, day_number):
        """选择具体日期"""
        try:
            print(f"步骤: 选择日期 {day_number} 日")
            
            day_cell = self.wait.until(
                EC.element_to_be_clickable((By.XPATH, f"//td[contains(@class, 'available')]//span[text()='{day_number}']"))
            )
            day_cell.click()
            print(f"✓ 日期 {day_number} 选择成功")
            time.sleep(2)
            
            return True
            
        except Exception as e:
            print(f"❌ 选择日期失败: {e}")
            return False
    
    def select_region(self, region_name="广东"):
        """选择地区"""
        try:
            print("步骤: 选择地区")
            
            region_arrow = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".el-select__caret.el-icon-arrow-up"))
            )
            region_arrow.click()
            print("✓ 地区下拉箭头点击成功")
            time.sleep(2)
            
            region_item = self.wait.until(
                EC.element_to_be_clickable((By.XPATH, f"//li[contains(@class, 'el-select-dropdown__item')]//span[text()='{region_name}']"))
            )
            region_item.click()
            print(f"✓ 地区 {region_name} 选择成功")
            time.sleep(2)
            
            return True
            
        except Exception as e:
            print(f"❌ 选择地区失败: {e}")
            return False
    
    def refresh_page(self):
        """刷新页面"""
        try:
            print("步骤: 刷新页面")
            
            refresh_button = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".el-icon-refresh-right"))
            )
            refresh_button.click()
            print("✓ 刷新按钮点击成功")
            time.sleep(3)
            
            return True
            
        except Exception as e:
            print(f"❌ 刷新失败: {e}")
            return False
    
    def process_single_date(self, month_name, day_number, region_name="广东"):
        """处理单个日期的完整流程"""
        date_info = f"2025{month_name}{day_number}日_{region_name}"
        self.download_count += 1
        
        print(f"\n{'='*50}")
        print(f"开始处理 [{self.download_count}]: {date_info}")
        print(f"{'='*50}")
        
        try:
            # 选择月份
            if not self.select_month(month_name):
                return False
            
            # 选择日期
            if not self.select_day(day_number):
                return False
            
            # 选择地区
            if not self.select_region(region_name):
                return False
            
            # 导出并保存xlsx文件
            file_path = self.click_export_and_save(date_info)
            
            if not file_path:
                return False
            
            # 等待一下确保导出完成
            time.sleep(3)
            
            # 刷新页面
            if not self.refresh_page():
                return False
            
            print(f"🎉 成功处理: {date_info}")
            return True
            
        except Exception as e:
            print(f"❌ 处理失败 {date_info}: {e}")
            self.failed_count += 1
            return False
    
    def download_entire_year(self, year="2025", regions=["广东"]):
        """下载整年数据"""
        try:
            print(f"开始下载 {year} 年全年数据")
            
            # 打开网站
            self.driver.get("https://spot.poweremarket.com/uptspot/sr/mp/portaladmin/index.html#/")
            time.sleep(5)
            
            # 首先导航到目标页面（只需要运行一次）
            if not self.navigate_to_target_page():
                print("❌ 导航失败，退出")
                return
            
            # 然后选择年份
            if not self.select_year(year):
                print("❌ 年份选择失败，退出")
                return
            
            # 定义所有月份和天数
            months = ["一月", "二月", "三月", "四月", "五月", "六月", 
                     "七月", "八月", "九月", "十月", "十一月", "十二月"]
            
            month_days = {
                "一月": 31, "二月": 28, "三月": 31, "四月": 30, "五月": 31, "六月": 30,
                "七月": 31, "八月": 31, "九月": 30, "十月": 31, "十一月": 30, "十二月": 31
            }
            
            total_days = sum(month_days.values()) * len(regions)
            processed_count = 0
            
            # 遍历所有地区和月份
            for region in regions:
                print(f"\n开始处理地区: {region}")
                
                for month in months:
                    days_in_month = month_days[month]
                    print(f"\n开始处理 {month} ({days_in_month}天)")
                    
                    # 遍历该月的每一天
                    for day in range(1, days_in_month + 1):
                        try:
                            success = self.process_single_date(month, str(day), region)
                            
                            if success:
                                processed_count += 1
                                progress = (processed_count / total_days) * 100
                                print(f"总体进度: {processed_count}/{total_days} ({progress:.1f}%)")
                            else:
                                print(f"❌ 跳过 {month} {day}日 - {region}")
                            
                            # 防止请求过快
                            time.sleep(2)
                            
                        except Exception as e:
                            print(f"❌ 处理 {month} {day}日时出错: {e}")
                            continue
            
            print(f"\n🎉 下载完成! 成功: {self.success_count}, 失败: {self.failed_count}")
            
        except Exception as e:
            print(f"❌ 下载过程出错: {e}")
    
    def download_specific_range(self, start_month=1, end_month=3, year="2025", regions=["广东"]):
        """下载指定月份范围的数据（用于测试）"""
        try:
            print(f"开始下载 {year} 年 {start_month}-{end_month} 月数据")
            
            self.driver.get("https://spot.poweremarket.com/uptspot/sr/mp/portaladmin/index.html#/")
            time.sleep(5)
            
            # 首先导航到目标页面（只需要运行一次）
            if not self.navigate_to_target_page():
                print("❌ 导航失败，退出")
                return
            
            # 然后选择年份
            if not self.select_year(year):
                return
            
            month_names = ["一月", "二月", "三月", "四月", "五月", "六月", 
                          "七月", "八月", "九月", "十月", "十一月", "十二月"]
            
            month_days = {
                "一月": 31, "二月": 28, "三月": 31, "四月": 30, "五月": 31, "六月": 30,
                "七月": 31, "八月": 31, "九月": 30, "十月": 31, "十一月": 30, "十二月": 31
            }
            
            target_months = month_names[start_month-1:end_month]
            
            for region in regions:
                print(f"\n处理地区: {region}")
                
                for month in target_months:
                    days_in_month = month_days[month]
                    test_days = min(3, days_in_month)  # 只测试前3天
                    
                    print(f"\n测试处理 {month} (前{test_days}天)")
                    
                    for day in range(1, test_days + 1):
                        try:
                            self.process_single_date(month, str(day), region)
                            time.sleep(2)
                        except Exception as e:
                            print(f"❌ 处理 {month} {day}日时出错: {e}")
                            continue
            
            print(f"\n🎉 测试下载完成! 成功: {self.success_count}, 失败: {self.failed_count}")
            
        except Exception as e:
            print(f"❌ 下载过程出错: {e}")
    
    def get_download_stats(self):
        """获取下载统计信息"""
        if os.path.exists(self.base_download_dir):
            xlsx_files = [f for f in os.listdir(self.base_download_dir) if f.endswith('.xlsx')]
            return len(xlsx_files)
        return 0
    
    def close(self):
        """关闭浏览器并显示统计信息"""
        total_files = self.get_download_stats()
        print(f"\n{'='*50}")
        print("下载统计:")
        print(f"总尝试次数: {self.download_count}")
        print(f"成功下载: {self.success_count}")
        print(f"失败次数: {self.failed_count}")
        print(f"实际保存xlsx文件数: {total_files}")
        print(f"文件保存位置: {self.base_download_dir}")
        print(f"{'='*50}")
        
        self.driver.quit()

# 使用示例
if __name__ == "__main__":
    downloader = CompleteDataDownloader("~/Downloads/2025_power_market_data")
    
    try:
        # 方法1: 完整下载全年数据（需要很长时间）
        # downloader.download_entire_year("2025", ["广东"])
        
        # 方法2: 测试下载前3个月的前3天
        downloader.download_specific_range(start_month=1, end_month=3, year="2025", regions=["广东"])
        
    except KeyboardInterrupt:
        print("\n用户中断下载")
    except Exception as e:
        print(f"程序执行出错: {e}")
    finally:
        downloader.close()