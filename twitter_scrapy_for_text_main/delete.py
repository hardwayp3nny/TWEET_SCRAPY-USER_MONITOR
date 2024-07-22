import os
import json
import csv
import pyodbc
from datetime import datetime
import pandas as pd

# 读取settings.json文件
with open('settings.json', 'r', encoding='utf8') as f:
    settings = json.load(f)
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=localhost;DATABASE=twitter;UID=1;PWD=1')
cursor = conn.cursor()

def get_latest_csv(folder_path):
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    if not csv_files:
        return None
    return max(csv_files, key=lambda x: os.path.getctime(os.path.join(folder_path, x)))

def create_table_if_not_exists(cursor, table_name):
    cursor.execute(f"""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' AND xtype='U')
    CREATE TABLE {table_name} (
        tweet_date DATETIME,
        tweet_url NVARCHAR(MAX),
        tweet_content NVARCHAR(MAX),
        favorite_count INT,
        retweet_count INT,
        reply_count INT,
        author NVARCHAR(255)
    )
    """)
    conn.commit()

def insert_data_from_csv(cursor, table_name, csv_file):
    try:
        # 跳过前三行，使用第四行作为列名
        df = pd.read_csv(csv_file, skiprows=3, encoding='utf-8-sig')
        print(f"CSV file read successfully. Columns: {df.columns}")

        for _, row in df.iterrows():
            cursor.execute(f"""
            INSERT INTO {table_name} 
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            datetime.strptime(row['Tweet Date'], '%Y/%m/%d %H:%M'),
            row['Tweet URL'],
            row['Tweet Content'],
            row['Favorite Count'],
            row['Retweet Count'],
            row['Reply Count'],
            row['Author'] if 'Author' in row else 'Unknown'
            )
        conn.commit()
        print(f"Inserted {len(df)} rows into {table_name}")
    except Exception as e:
        print(f"Error processing file {csv_file}: {str(e)}")
        raise

# 主程序
save_path = settings['save_path']
user_list = settings['user_lst'].split(',')

for user in user_list:
    user_folder = os.path.join(save_path, user)
    if not os.path.exists(user_folder):
        print(f"Folder for user {user} not found.")
        continue

    latest_csv = get_latest_csv(user_folder)
    if not latest_csv:
        print(f"No CSV file found for user {user}.")
        continue

    table_name = f"Twitter_{user}"
    create_table_if_not_exists(cursor, table_name)

    csv_path = os.path.join(user_folder, latest_csv)
    insert_data_from_csv(cursor, table_name, csv_path)
    print(f"Data inserted for user {user} from file {latest_csv}")

# 关闭数据库连接
cursor.close()
conn.close()