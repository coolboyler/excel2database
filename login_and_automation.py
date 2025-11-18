#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
电力市场数据自动化获取脚本
用于登录并执行后续自动化操作（专为Safari浏览器设计）

重要提示：
使用此脚本前必须完成以下配置：
1. 在终端执行命令启用Safari驱动: safaridriver --enable
2. 在Safari浏览器中启用"开发"菜单下的"允许远程自动化"选项
3. 如果遇到证书问题，需要通过钥匙串添加并信任相关证书

Safari浏览器特殊说明：
- Safari不支持无头模式运行
- 必须在Safari浏览器中手动完成登录过程
- Safari自动化期间，用户可以与浏览器窗口交互，但不能切换到其他应用
- 浏览器窗口会自动最大化以确保登录界面完整显示
"""

import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.safari.options import Options as SafariOptions
from selenium.webdriver.common.keys import Keys
import logging
import sys
import os

# 添加项目根目录到Python路径，以便导入safari_automation模块
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 尝试导入safari_automation模块
try:
    from safari_automation import CompleteDataDownloader
    SAFARI_AUTOMATION_AVAILABLE = True
except ImportError:
    SAFARI_AUTOMATION_AVAILABLE = False
    print("警告: 无法导入safari_automation模块，登录成功后将不会执行自动化下载任务")

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PowerMarketAutomation:
    def __init__(self, headless=False):
        """
        初始化自动化浏览器
        :param headless: 是否以无头模式运行（注意：Safari不支持无头模式）
        """
        self.driver = None
        self.headless = headless
        self.login_url = "https://spot.poweremarket.com/uptspot/sr/pt/login.html#/"
        self.main_url = "https://spot.poweremarket.com/uptspot/sr/mp/portaladmin/index.html#/"
        
    def setup_driver(self):
        """
        设置Safari浏览器驱动
        注意：使用Safari进行自动化需要先在终端执行以下命令启用驱动：
        safaridriver --enable
        
        同时需要在Safari的"开发"菜单中启用"允许远程自动化"选项
        """
        # Safari不支持headless模式，忽略headless参数
        if self.headless:
            logger.warning("Safari浏览器不支持无头模式，将忽略headless参数")
            
        safari_options = SafariOptions()
        
        try:
            self.driver = webdriver.Safari(options=safari_options)
            # 设置浏览器窗口大小，确保能完整显示登录页面
            self.driver.set_window_size(1920, 1080)
            self.driver.maximize_window()
            logger.info("Safari浏览器驱动已初始化，窗口大小已设置")
        except Exception as e:
            logger.error(f"初始化Safari驱动失败: {e}")
            logger.info("请确保已执行以下配置：")
            logger.info("1. 在终端执行: safaridriver --enable")
            logger.info("2. 在Safari浏览器中启用'开发'菜单下的'允许远程自动化'选项")
            raise
            
    def wait_for_login(self, timeout=300):
        """
        等待用户手动登录
        :param timeout: 等待超时时间（秒）
        """
        logger.info("请在浏览器中手动登录...")
        logger.info(f"登录页面: {self.login_url}")
        logger.info("等待检测到登录成功...")
        logger.info("提示：您可以在Safari浏览器窗口中输入账号密码进行登录")
        logger.info("注意：登录过程中请勿切换到其他应用程序")
        
        self.driver.get(self.login_url)
        # 确保窗口大小合适
        self.driver.set_window_size(1920, 1080)
        self.driver.maximize_window()
        
        # 尝试定位用户名输入框，帮助用户确认页面已加载
        try:
            wait = WebDriverWait(self.driver, 10)
            username_field = wait.until(
                EC.presence_of_element_located((By.XPATH, "//input[@type='text' and contains(@placeholder, '用户名') or contains(@placeholder, '账号') or @name='username' or @id='username']"))
            )
            logger.info("登录页面已加载，您可以开始输入账号密码")
        except:
            logger.info("正在加载登录页面，请稍候...")
        
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                # 检查登录状态
                if self.check_login_status():
                    logger.info("检测到登录成功!")
                    return True
                
                # 等待一小段时间再检查
                time.sleep(1)
            except Exception as e:
                logger.warning(f"检查登录状态时出错: {e}")
                time.sleep(1)
                
        logger.error("等待登录超时")
        return False
            
    def check_login_status(self):
        """
        检查当前是否已登录
        使用多种方法检测登录状态以提高可靠性
        """
        try:
            current_url = self.driver.current_url
            
            # 方法1: 检查URL是否已从登录页面跳转到主页
            if "login" not in current_url and "portaladmin" in current_url:
                logger.debug("通过URL变化检测到已登录状态")
                return True
                
            # 方法2: 检查页面标题是否包含登录后的特征
            page_title = self.driver.title
            if "管理" in page_title or "Admin" in page_title or "主页" in page_title:
                logger.debug("通过页面标题检测到已登录状态")
                return True
                
            # 方法3: 检查页面是否包含特定的已登录元素
            try:
                # 查找可能的用户信息显示区域
                user_elements = self.driver.find_elements(By.XPATH, 
                    "//span[contains(text(), '欢迎') or contains(text(), 'Welcome') or contains(@class, 'username') or contains(@class, 'user')] | "
                    "//div[contains(@class, 'user') and not(contains(@class, 'login'))] | "
                    "//a[contains(@href, 'logout') or contains(text(), '退出') or contains(text(), 'Logout')]"
                )
                
                for element in user_elements:
                    if element.is_displayed() and element.text.strip():
                        logger.debug(f"通过页面元素检测到已登录状态: {element.text}")
                        return True
            except:
                # 未找到相关元素
                pass
                
            logger.debug("当前未检测到登录状态")
            return False
        except Exception as e:
            logger.error(f"检查登录状态时出错: {e}")
            return False
            
    def wait_for_page_load(self, timeout=10):
        """
        等待页面加载完成
        :param timeout: 超时时间（秒）
        :return: 是否加载完成
        """
        try:
            wait = WebDriverWait(self.driver, timeout)
            
            # 等待页面文档状态为完成
            wait.until(lambda driver: driver.execute_script("return document.readyState") == "complete")
            
            # 额外等待一小段时间确保页面渲染完成
            time.sleep(0.5)
            
            logger.debug("页面加载完成")
            return True
        except Exception as e:
            logger.warning(f"等待页面加载时出错: {e}")
            return False
            
    def perform_automation_tasks(self):
        """
        执行自动化任务
        这里可以添加你需要的具体操作
        """
        logger.info("开始执行自动化任务...")
        
        # 确保已经登录
        if not self.check_login_status():
            logger.error("未登录，无法执行自动化任务")
            return False
            
        # 等待页面加载完成
        if not self.wait_for_page_load():
            logger.warning("页面可能未完全加载，继续执行任务")
            
        # 在这里添加你的具体自动化操作
        logger.info("在这里执行你的自动化任务...")
        
        # 示例：等待并点击某个元素
        try:
            # 等待页面加载完成
            wait = WebDriverWait(self.driver, 10)
            
            # 示例操作 - 根据实际需要修改
            # 1. 等待某个特定元素出现，确认页面已完全加载
            # page_loaded = wait.until(
            #     EC.presence_of_element_located((By.CLASS_NAME, "main-content"))
            # )
            
            # 2. 查找并点击特定按钮或链接
            # button = wait.until(
            #     EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), '数据查询')]"))
            # )
            # button.click()
            
            # 3. 等待结果加载
            # result = wait.until(
            #     EC.presence_of_element_located((By.CLASS_NAME, "result-table"))
            # )
            
            # 4. 提取数据或执行其他操作
            # data_elements = self.driver.find_elements(By.XPATH, "//table[@class='result-table']//td")
            # for element in data_elements:
            #     logger.info(f"数据项: {element.text}")
            
            logger.info("自动化任务执行完成")
            return True
            
        except Exception as e:
            logger.error(f"执行自动化任务时出错: {e}")
            return False
            
    def run(self):
        """
        运行主流程
        """
        try:
            # 设置驱动
            self.setup_driver()
            
            print("\n浏览器已启动，请在打开的Safari浏览器中完成以下操作：")
            print("1. 在登录页面输入用户名和密码")
            print("2. 点击登录按钮")
            print("3. 等待系统检测到登录成功")
            print("注意：您可以在Safari浏览器窗口中进行登录操作，但请勿切换到其他应用程序")
            print()
            
            # 等待用户登录
            if self.wait_for_login():
                print("登录成功！现在可以执行自动化任务...")
                # 登录成功后，调用safari_automation模块执行后续任务
                if SAFARI_AUTOMATION_AVAILABLE:
                    print("开始执行数据下载任务...")
                    try:
                        # 创建CompleteDataDownloader实例并执行任务
                        downloader = CompleteDataDownloader()
                        # 复用当前已登录的driver
                        downloader.driver = self.driver
                        downloader.wait = WebDriverWait(self.driver, 15)
                        # 执行下载任务（测试范围）
                        downloader.download_specific_range(start_month=1, end_month=1, year="2025", regions=["广东"])
                        print("数据下载任务执行完成！")
                    except Exception as e:
                        logger.error(f"执行自动化下载任务时出错: {e}")
                        print("警告: 自动化下载任务执行失败")
                else:
                    print("警告: 未找到safari_automation模块，跳过自动化下载任务")
                
                # 执行其他自动化任务
                self.perform_automation_tasks()
            else:
                logger.error("登录失败或超时")
                
        except Exception as e:
            logger.error(f"运行过程中出错: {e}")
            
        finally:
            # 保持浏览器打开以便查看结果
            print("\n自动化任务已完成。")
            input("按回车键关闭浏览器...")
            if self.driver:
                self.driver.quit()
                
    def close(self):
        """
        关闭浏览器
        """
        if self.driver:
            self.driver.quit()
            logger.info("浏览器已关闭")

def main():
    """
    主函数
    """
    print("电力市场数据自动化工具")
    print("=" * 50)
    print("重要提示：")
    print("1. 使用前请确保已执行: safaridriver --enable")
    print("2. 在Safari中启用'开发'->'允许远程自动化'")
    print("3. 登录过程中请勿切换到其他应用程序")
    print("4. 浏览器窗口已最大化以确保正常显示登录界面")
    print()
    print("关于登录操作的说明：")
    print("- 浏览器启动后会自动跳转到登录页面")
    print("- 您可以在Safari浏览器窗口中直接输入账号密码")
    print("- Safari自动化期间，您可以与当前浏览器窗口交互")
    print("- 请勿切换到其他应用程序，这可能会中断自动化流程")
    print("- 登录成功后（URL从登录页跳转到主页），脚本会自动检测并继续执行后续任务")
    print()
    
    input("确认已完成上述配置后，按回车键启动浏览器...")
    
    # Safari不支持无头模式
    automation = PowerMarketAutomation(headless=False)
    automation.run()

if __name__ == "__main__":
    main()