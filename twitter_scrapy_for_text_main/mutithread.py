import subprocess
import time
import os
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_script(script_name):
    logging.info(f"正在运行 {script_name}")
    try:
        subprocess.run(["python", script_name], check=True)
        logging.info(f"{script_name} 运行成功")
    except subprocess.CalledProcessError as e:
        logging.error(f"运行 {script_name} 时发生错误: {e}")
        logging.error(f"错误输出: {e.output}")
    except FileNotFoundError:
        logging.error(f"找不到文件: {script_name}")
        logging.error(f"当前工作目录: {os.getcwd()}")
        logging.error(f"目录内容: {os.listdir('.')}")

def run_cycle():
    try:
        run_script("text.py")
        run_script("csv_save_db.py")
        logging.info("周期完成")
    except Exception as e:
        logging.exception(f"运行周期时发生意外错误: {e}")

def main():
    logging.info(f"开始运行 mutithread main 函数")
    logging.info(f"当前工作目录: {os.getcwd()}")
    run_cycle()
    logging.info(f"mutithread main 函数运行完毕")

if __name__ == "__main__":
    main()