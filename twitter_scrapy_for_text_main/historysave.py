import os
import glob
import pandas as pd
import pyodbc
import csv
from io import StringIO


def get_latest_csv(username, base_path):
    user_folder = os.path.join(base_path, username)
    csv_files = glob.glob(os.path.join(user_folder, '*.csv'))
    if not csv_files:
        return None
    return max(csv_files, key=os.path.getmtime)


def read_csv_manually(file_path):
    with open(file_path, 'r', encoding='utf-8-sig') as file:
        content = file.read()

    csv_io = StringIO(content)
    reader = csv.reader(csv_io)

    # 读取前两行
    header_row = next(reader)
    second_row = next(reader)

    print("Header row:", header_row)
    print("Second row:", second_row)

    # 使用第二行作为列名
    if len(second_row) >= 6:
        headers = ['Tweet Date', 'Tweet URL', 'Tweet Content', 'Favorite Count', 'Retweet Count', 'Reply Count']
    else:
        headers = [f'Column_{i}' for i in range(len(second_row))]

    # 准备数据列表，从第二行开始
    data = [second_row]
    for row in reader:
        # 确保每行都有6个元素，如果不足则用空字符串填充
        padded_row = row + [''] * (6 - len(row))
        data.append(padded_row[:6])  # 只取前6个元素

    df = pd.DataFrame(data, columns=headers)

    print("DataFrame info:")
    print(df.info())
    print("\nDataFrame head:")
    print(df.head())

    return df

def insert_data_to_sql(df, table_name, connection):
    cursor = connection.cursor()

    # 创建表（如果不存在）
    columns = [f"[{col}] NVARCHAR(MAX)" for col in df.columns]
    create_table_query = f"IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' AND xtype='U') CREATE TABLE {table_name} ({', '.join(columns)})"
    cursor.execute(create_table_query)

    # 插入数据
    for _, row in df.iterrows():
        placeholders = ', '.join(['?' for _ in row])
        insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
        cursor.execute(insert_query, tuple(row))

    connection.commit()


def main(usernames, base_path):
    conn_str = 'DRIVER={SQL Server};SERVER=localhost;DATABASE=twitter;UID=1;PWD=1'
    connection = pyodbc.connect(conn_str)

    for username in usernames:
        latest_csv = get_latest_csv(username, base_path)
        if latest_csv:
            print(f"Processing {username}'s latest CSV: {latest_csv}")
            df = read_csv_manually(latest_csv)

            # 打印DataFrame的前几行和列名，用于调试
            print(df.head())
            print(df.columns)

            insert_data_to_sql(df, username, connection)
            print(f"Data inserted into table '{username}' successfully.")
        else:
            print(f"No CSV file found for {username}")

    connection.close()


if __name__ == "__main__":
    base_path = r"C:\Users\86158\Desktop\推特爬取工具包含消息侦测\twitter_scrapy_for_text-main"
    usernames = ["elonmusk"]  # 您可以在这里添加更多用户名
    main(usernames, base_path)