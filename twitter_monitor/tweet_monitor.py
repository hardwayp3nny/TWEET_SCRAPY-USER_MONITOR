from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from winotify import Notification, audio
import time
import logging
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def login_twitter_with_cookies(driver):
    driver.get("https://twitter.com")

    cookies = [
        {'name': 'auth_token', 'value': '8cf2442db9ac3d5f1b2459c8c3eaae5ae8bd0828'},
        {'name': 'ct0',
         'value': '5a78bba6b07cd976596023763312f4e272ad9a99bce3b46e98e22771578ae008bc46711b0293c7fa681aa7773b4657d6893d03da0f2dc0ba9a86aa9151449b918f1f75f0c0d203e18dbf8375b4173048'}
    ]

    for cookie in cookies:
        driver.add_cookie(cookie)

    driver.refresh()
    logging.info("已添加登录 cookies")

def send_windows_notification(title, message):
    notification = Notification(app_id="Twitter通知监控", title=title, msg=message, duration="short")
    notification.set_audio(audio.Default, loop=False)
    notification.show()

def check_notification(driver):
    try:
        xpath = "//div[@aria-live='polite' and contains(@aria-label, '条未读项目')]"
        element = driver.find_element(By.XPATH, xpath)

        label = element.get_attribute('aria-label')
        match = re.search(r'(\d+)\s*条未读项目', label)
        if match:
            unread_count = int(match.group(1))
            return unread_count
        return 0
    except NoSuchElementException:
        return 0

def open_notifications_and_return(driver):
    driver.get("https://x.com/notifications")
    time.sleep(5)  # 等待页面加载
    driver.get("https://twitter.com")  # 返回主页

def monitor_notifications(driver):
    notification_count = check_notification(driver)
    if notification_count > 0:
        logging.info(f"检测到{notification_count}条新通知！")
        send_windows_notification("Twitter新通知", f"您有{notification_count}条新的Twitter通知，请查看。")
        open_notifications_and_return(driver)
        return True
    else:
        logging.info("没有检测到新通知")
        return False

def wait_for_element(driver, by, value, timeout=10):
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((by, value))
    )

def is_logged_in(driver):
    try:
        wait_for_element(driver, By.CSS_SELECTOR, '[data-testid="AppTabBar_Home_Link"]', timeout=5)
        return True
    except:
        return False

def test_twitter_login():
    driver = setup_driver()
    try:
        login_twitter_with_cookies(driver)
        if is_logged_in(driver):
            print("登录测试成功")
        else:
            print("登录测试失败")
    finally:
        driver.quit()

if __name__ == "__main__":
    test_twitter_login()