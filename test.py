from selenium import webdriver
from selenium.webdriver.common.by import By
import time

def smart_export():
    current_url = None
    
    try:
        # 尝试获取当前页面的URL
        current_url = driver.current_url
        print(f"当前页面: {current_url}")
    except:
        # 如果driver不存在，创建新的
        driver = webdriver.Safari()
        driver.get("https://spot.poweremarket.com/uptspot/sr/mp/portaladmin/index.html#/")
        print("启动了新的浏览器会话")
    else:
        # 如果driver存在，检查是否在目标页面
        target_url = "https://spot.poweremarket.com/uptspot/sr/mp/portaladmin/index.html"
        if target_url not in current_url:
            print("不在目标页面，正在跳转...")
            driver.get("https://spot.poweremarket.com/uptspot/sr/mp/portaladmin/index.html#/")
            time.sleep(3)
    
    # 执行导出操作
    try:
        export_button = driver.find_element(By.CSS_SELECTOR, 'button.el-button.s1.el-button--primary')
        driver.execute_script("arguments[0].click();", export_button)
        print("✅ 导出按钮点击成功！")
        return True
    except Exception as e:
        print(f"❌ 点击失败: {e}")
        return False

# 执行函数
smart_export()