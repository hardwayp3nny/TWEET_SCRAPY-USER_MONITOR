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
import subprocess

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


def run_crawler():
    try:
        subprocess.run(["python", "twitter_crawler.py"], check=True)
    except subprocess.CalledProcessError as e:
        logging.error(f"运行爬虫脚本时发生错误: {e}")


def monitor_notifications(driver):
    while True:
        notification_count = check_notification(driver)

        if notification_count > 0:
            logging.info(f"检测到{notification_count}条新通知！")
            send_windows_notification("Twitter新通知", f"您有{notification_count}条新的Twitter通知，请查看。")

            open_notifications_and_return(driver)
            #run_external_script()
            run_crawler()
            # 重置循环
            continue

        logging.info("没有检测到新通知，10秒后重新检查")
        time.sleep(10)


def main():
    driver = setup_driver()
    wait = WebDriverWait(driver, 20)

    try:
        login_twitter_with_cookies(driver)
        logging.info("正在验证登录状态...")

        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="AppTabBar_Home_Link"]')))
        logging.info("登录成功，开始监控通知...")

        monitor_notifications(driver)
    except Exception as e:
        logging.error(f"发生错误: {e}")
    finally:
        driver.quit()


if __name__ == "__main__":
    main()