import sys
import os
import time

# 添加必要的路径
sys.path.append(r"C:\Users\86158\Desktop\推特爬取工具包含消息侦测")

from twitter_scrapy_for_text_main import mutithread
from twitter_monitor import tweet_monitor

def main():
    driver = tweet_monitor.setup_driver()
    tweet_monitor.login_twitter_with_cookies(driver)

    # 获取 mutithread.py 所在的目录
    mutithread_dir = os.path.dirname(os.path.abspath(mutithread.__file__))

    try:
        while True:
            if tweet_monitor.monitor_notifications(driver):
                # 改变当前工作目录到 mutithread.py 所在的目录
                original_dir = os.getcwd()
                os.chdir(mutithread_dir)
                try:
                    mutithread.main()
                finally:
                    # 恢复原来的工作目录
                    os.chdir(original_dir)
            time.sleep(10)  # 每10秒检查一次
    except Exception as e:
        print(f"发生错误: {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()