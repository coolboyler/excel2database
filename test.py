import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def quick_test():
    """快速测试单个日期流程"""
    driver = webdriver.Safari()
    wait = WebDriverWait(driver, 15)
    
    try:
        # 打开网站
        driver.get("https://spot.poweremarket.com/uptspot/sr/mp/portaladmin/index.html#/")
        time.sleep(5)
        
        print("1. 选择年份")
        date_input = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input.el-input__inner")))
        date_input.click()
        time.sleep(2)
        
        year_header = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".el-date-picker__header-label")))
        year_header.click()
        time.sleep(2)
        
        year_2025 = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(@class, 'cell') and text()='2025']")))
        year_2025.click()
        time.sleep(2)
        
        print("2. 选择一月")
        january = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(@class, 'cell') and text()='一月']")))
        january.click()
        time.sleep(2)
        
        print("3. 选择1日")
        day_1 = wait.until(EC.element_to_be_clickable((By.XPATH, "//td[contains(@class, 'available')]//span[text()='1']")))
        day_1.click()
        time.sleep(2)
        
        print("4. 选择地区")
        region_arrow = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".el-select__caret.el-icon-arrow-up")))
        region_arrow.click()
        time.sleep(2)
        
        guangdong = wait.until(EC.element_to_be_clickable((By.XPATH, "//li[contains(@class, 'el-select-dropdown__item')]//span[text()='广东']")))
        guangdong.click()
        time.sleep(2)
        
        print("5. 点击导出")
        export_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='导出']]")))
        export_btn.click()
        time.sleep(10)
        
        print("6. 刷新页面")
        refresh_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".el-icon-refresh-right")))
        refresh_btn.click()
        time.sleep(5)
        
        print("🎉 测试完成!")
        
    except Exception as e:
        print(f"测试失败: {e}")
        driver.save_screenshot("test_error.png")
    finally:
        driver.quit()

# 运行测试
quick_test()