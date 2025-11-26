import time
import os
import shutil
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.safari.options import Options

class CompleteDataDownloader:
    def __init__(self, base_download_dir="~/Downloads/2025_power_data"):
        # 直接连接到已打开的Safari实例
        self.driver = self.connect_to_existing_browser()
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
    
    def connect_to_existing_browser(self):
        """连接到已经打开的Safari浏览器"""
        try:
            # 方法1: 使用Safari远程调试模式
            safari_options = Options()
            safari_options.debugger_address = "127.0.0.1:27753"
            driver = webdriver.Safari(options=safari_options)
            print("✅ 成功连接到已打开的Safari浏览器")
            
            # 显示当前所有标签页信息
            handles = driver.window_handles
            print(f"📑 发现 {len(handles)} 个标签页:")
            for i, handle in enumerate(handles):
                driver.switch_to.window(handle)
                print(f"  {i+1}. {driver.title} - {driver.current_url}")
            
            # 切换到第一个标签页（通常是您正在查看的页面）
            driver.switch_to.window(handles[0])
            print(f"🎯 已切换到标签页: {driver.title}")
            
            return driver
            
        except Exception as e:
            print(f"❌ 无法连接到已打开的浏览器: {e}")
            print("💡 请确保已启用Safari远程调试:")
            print("   1. 打开Safari → 偏好设置 → 高级")
            print("   2. 勾选「在菜单栏中显示开发菜单」")
            print("   3. 在终端运行: /Applications/Safari.app/Contents/MacOS/Safari --remote-debugging-port=27753")
            raise
    
    def ensure_current_page_is_target(self):
        """确保当前页面是目标页面"""
        try:
            current_url = self.driver.current_url
            target_url = "https://spot.poweremarket.com/uptspot/sr/mp/portaladmin/index.html"
            
            if target_url in current_url:
                print("✅ 当前页面已经是目标页面")
                return True
            else:
                print(f"⚠️ 当前页面不是目标页面: {current_url}")
                print("🔄 正在检查其他标签页...")
                
                # 在所有标签页中寻找目标页面
                handles = self.driver.window_handles
                target_handle = None
                
                for handle in handles:
                    self.driver.switch_to.window(handle)
                    if target_url in self.driver.current_url:
                        target_handle = handle
                        print(f"✅ 在标签页中找到目标页面: {self.driver.title}")
                        break
                
                if target_handle:
                    self.driver.switch_to.window(target_handle)
                    return True
                else:
                    print("❌ 在所有标签页中都未找到目标页面")
                    print("🔗 正在导航到目标页面...")
                    self.driver.get("https://spot.poweremarket.com/uptspot/sr/mp/portaladmin/index.html#/")
                    time.sleep(5)
                    return True
                    
        except Exception as e:
            print(f"❌ 检查页面失败: {e}")
            return False
    
    def smart_click_export(self):
        """智能点击导出按钮"""
        try:
            print("🔍 寻找导出按钮...")
            
            # 尝试多种选择器
            selectors = [
                'button.el-button.s1.el-button--primary',
                'button[class*="el-button--primary"]',
                '//button[contains(@class, "el-button--primary")]//span[text()="导出"]/..',
                '//button[.//span[text()="导出"]]',
                '//span[text()="导出"]/ancestor::button',
                '//button[contains(@class, "s1")]',
                '//*[contains(text(), "导出") and (self::button or self::span)]/ancestor-or-self::button'
            ]
            
            for i, selector in enumerate(selectors):
                try:
                    if selector.startswith('//'):
                        element = self.driver.find_element(By.XPATH, selector)
                    else:
                        element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    
                    print(f"✅ 找到导出按钮 (选择器 {i+1}: {selector})")
                    
                    # 检查按钮状态
                    is_disabled = element.get_attribute('disabled')
                    has_disabled_class = 'is-disabled' in element.get_attribute('class')
                    
                    if is_disabled or has_disabled_class:
                        print("⚠️ 按钮被禁用，尝试强制点击")
                        self.driver.execute_script("arguments[0].click();", element)
                    else:
                        print("🖱️ 按钮已启用，直接点击")
                        element.click()
                    
                    print("✅ 导出按钮点击成功！")
                    return True
                    
                except Exception as e:
                    print(f"❌ 选择器 {i+1} 失败: {e}")
                    continue
            
            print("❌ 所有选择器都找不到导出按钮")
            return False
            
        except Exception as e:
            print(f"❌ 点击导出按钮失败: {e}")
            return False

    def ensure_directories(self):
        """确保所有必要的目录都存在"""
        if not os.path.exists(self.base_download_dir):
            os.makedirs(self.base_download_dir)
            print(f"✓ 创建主目录: {self.base_download_dir}")
    
    def navigate_to_target_page(self):
        """导航到目标页面（只需要运行一次）"""
        try:
            # 首先确保在当前目标页面
            if not self.ensure_current_page_is_target():
                return False
            
            print("开始导航到目标功能页面...")
            
            # 步骤1: 点击"我的交易"
            print("步骤1: 点击'我的交易'")
            my_trade_selectors = [
                "//li[contains(@class, 'active')]//*[contains(text(), '我的交易')]",
                "//*[contains(text(), '我的交易')]",
                "//span[contains(text(), '我的交易')]"
            ]
            
            my_trade = None
            for selector in my_trade_selectors:
                try:
                    my_trade = self.wait.until(EC.element_to_be_clickable((By.XPATH, selector)))
                    print(f"✅ 找到'我的交易'按钮: {selector}")
                    break
                except:
                    continue
            
            if my_trade:
                my_trade.click()
                print("✓ '我的交易'点击成功")
                time.sleep(2)
            
            # 步骤2: 点击"实时交易"
            print("步骤2: 点击'实时交易'")
            realtime_selectors = [
                "//li[contains(@class, 'leftmenu-item')]//*[contains(text(), '实时交易')]",
                "//*[contains(text(), '实时交易')]",
                "//span[contains(text(), '实时交易')]"
            ]
            
            realtime_trade = None
            for selector in realtime_selectors:
                try:
                    realtime_trade = self.wait.until(EC.element_to_be_clickable((By.XPATH, selector)))
                    print(f"✅ 找到'实时交易'按钮: {selector}")
                    break
                except:
                    continue
            
            if realtime_trade:
                realtime_trade.click()
                print("✓ '实时交易'点击成功")
                time.sleep(2)
            
            # 步骤3: 点击"实时节点电价查询"
            print("步骤3: 点击'实时节点电价查询'")
            price_query_selectors = [
                "//span[contains(@class, 'title-text') and contains(text(), '实时节点电价查询')]",
                "//*[contains(text(), '实时节点电价查询')]",
                "//span[contains(text(), '实时节点电价查询')]"
            ]
            
            price_query = None
            for selector in price_query_selectors:
                try:
                    price_query = self.wait.until(EC.element_to_be_clickable((By.XPATH, selector)))
                    print(f"✅ 找到'实时节点电价查询'按钮: {selector}")
                    break
                except:
                    continue
            
            if price_query:
                price_query.click()
                print("✓ '实时节点电价查询'点击成功")
                time.sleep(2)
            
            print("🎉 导航完成，已进入目标功能页面")
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
            
            # 使用智能导出功能
            if self.smart_click_export():
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
            else:
                print("❌ 导出按钮点击失败")
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
            
            # 尝试多种日期输入框选择器
            date_selectors = [
                "input.el-input__inner[placeholder*='日期']",
                "input[placeholder*='日期']",
                ".el-date-editor input"
            ]
            
            date_input = None
            for selector in date_selectors:
                try:
                    date_input = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector)))
                    print(f"✅ 找到日期输入框: {selector}")
                    break
                except:
                    continue
            
            if date_input:
                date_input.click()
                print("✓ 日期输入框点击成功")
                time.sleep(2)
            
            # 点击年份选择
            year_header_selectors = [
                ".el-date-picker__header-label",
                ".el-date-picker__header button"
            ]
            
            year_header = None
            for selector in year_header_selectors:
                try:
                    year_header = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector)))
                    print(f"✅ 找到年份选择按钮: {selector}")
                    break
                except:
                    continue
            
            if year_header:
                year_header.click()
                print("✓ 年份选择按钮点击成功")
                time.sleep(2)
            
            # 选择具体年份
            year_xpath = f"//a[contains(@class, 'cell') and text()='{target_year}']"
            year_cell = self.wait.until(EC.element_to_be_clickable((By.XPATH, year_xpath)))
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
    
    def download_specific_range(self, start_month=1, end_month=3, year="2025", regions=["广东"]):
        """下载指定月份范围的数据"""
        try:
            print(f"开始下载 {year} 年 {start_month}-{end_month} 月数据")
            
            # 确保在当前目标页面
            if not self.ensure_current_page_is_target():
                print("❌ 无法确保在目标页面")
                return
            
            # 首先导航到目标功能页面
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
        """显示统计信息，但不关闭浏览器"""
        total_files = self.get_download_stats()
        print(f"\n{'='*50}")
        print("下载统计:")
        print(f"总尝试次数: {self.download_count}")
        print(f"成功下载: {self.success_count}")
        print(f"失败次数: {self.failed_count}")
        print(f"实际保存xlsx文件数: {total_files}")
        print(f"文件保存位置: {self.base_download_dir}")
        print("💡 浏览器保持打开状态，您可以继续使用")
        print(f"{'='*50}")

# 使用示例
if __name__ == "__main__":
    # 首先启用Safari远程调试（在终端中运行）
    # /Applications/Safari.app/Contents/MacOS/Safari --remote-debugging-port=27753
    
    downloader = CompleteDataDownloader("~/Downloads/2025_power_market_data")
    
    try:
        # 测试下载前3个月的前3天
        downloader.download_specific_range(start_month=1, end_month=3, year="2025", regions=["广东"])
        
    except KeyboardInterrupt:
        print("\n用户中断下载")
    except Exception as e:
        print(f"程序执行出错: {e}")
    finally:
        downloader.close()