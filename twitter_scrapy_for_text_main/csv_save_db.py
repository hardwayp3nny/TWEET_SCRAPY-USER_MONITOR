import os
import pyodbc
from datetime import datetime
import pandas as pd

def get_latest_csv(folder_path):
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    if not csv_files:
        return None
    return max(csv_files, key=lambda x: os.path.getctime(os.path.join(folder_path, x)))

def create_table_if_not_exists(cursor, table_name):
    cursor.execute(f"""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' AND xtype='U')
    CREATE TABLE {table_name} (
        tweet_time DATETIME,
        tweet_url NVARCHAR(MAX),
        tweet_content NVARCHAR(MAX),
        favorite_count INT,
        retweet_count INT,
        reply_count INT
    )
    """)
    conn.commit()

def safe_int_convert(value):
    try:
        return int(float(value))  # 先转换为float，再转换为int
    except (ValueError, TypeError):
        return 0


def insert_data_from_csv(cursor, table_name, csv_file):
    try:
        # 尝试读取CSV文件
        df = pd.read_csv(csv_file, encoding='utf-8-sig', skiprows=4, header=None)

        if not df.empty:
            print(f"CSV file read successfully. Columns: {df.columns}")
            print(f"First few rows: \n{df.head()}")

            # 为列指定名称
            df.columns = ['Tweet Date', 'Tweet URL', 'Tweet Content', 'Favorite Count', 'Retweet Count', 'Reply Count']

            inserted_count = 0
            updated_count = 0
            skipped_count = 0

            for _, row in df.iterrows():
                # 检查是否存在相同URL的记录
                cursor.execute(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE tweet_url = ?
                """, str(row['Tweet URL']))

                if cursor.fetchone()[0] == 0:
                    # 如果不存在相同URL的记录，则插入新记录
                    cursor.execute(f"""
                    INSERT INTO {table_name} 
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                                   datetime.strptime(str(row['Tweet Date']), '%Y-%m-%d %H:%M'),
                                   str(row['Tweet URL']),
                                   str(row['Tweet Content']),
                                   safe_int_convert(row['Favorite Count']),
                                   safe_int_convert(row['Retweet Count']),
                                   safe_int_convert(row['Reply Count'])
                                   )
                    inserted_count += 1
                else:
                    # 如果存在相同URL的记录，更新其他信息
                    cursor.execute(f"""
                    UPDATE {table_name}
                    SET tweet_time = ?, tweet_content = ?, 
                        favorite_count = ?, retweet_count = ?, reply_count = ?
                    WHERE tweet_url = ?
                    """,
                                   datetime.strptime(str(row['Tweet Date']), '%Y-%m-%d %H:%M'),
                                   str(row['Tweet Content']),
                                   safe_int_convert(row['Favorite Count']),
                                   safe_int_convert(row['Retweet Count']),
                                   safe_int_convert(row['Reply Count']),
                                   str(row['Tweet URL'])
                                   )
                    if cursor.rowcount > 0:
                        updated_count += 1
                    else:
                        skipped_count += 1

            conn.commit()
            print(
                f"Processed {len(df)} rows. Inserted: {inserted_count}, Updated: {updated_count}, Skipped: {skipped_count}")
        else:
            print(f"File {csv_file} is empty. Skipping.")
    except pd.errors.EmptyDataError:
        print(f"File {csv_file} is empty or has no parsable data. Skipping.")
    except Exception as e:
        print(f"Error processing file {csv_file}: {str(e)}")
        print(f"File path: {os.path.abspath(csv_file)}")
        print(f"File size: {os.path.getsize(csv_file)} bytes")

    print(f"Finished processing {csv_file}")


# ... [前面的代码保持不变] ...

def sort_table_by_time(cursor, table_name):
    cursor.execute(f"""
    CREATE TABLE {table_name}_temp (
        tweet_time DATETIME,
        tweet_url NVARCHAR(MAX),
        tweet_content NVARCHAR(MAX),
        favorite_count INT,
        retweet_count INT,
        reply_count INT
    )
    """)

    cursor.execute(f"""
    INSERT INTO {table_name}_temp
    SELECT TOP 1000000 *
    FROM {table_name}
    ORDER BY tweet_time DESC
    """)

    cursor.execute(f"DROP TABLE {table_name}")
    cursor.execute(f"EXEC sp_rename '{table_name}_temp', '{table_name}'")
    conn.commit()
    print(f"Table {table_name} has been sorted by tweet_time in descending order.")


# 主程序
base_path = r"C:\Users\86158\Desktop\推特爬取工具包含消息侦测\twitter_scrapy_for_text_main"
usernames = ["OnchainDataNerd", "elonmusk", "layerggofficial"]  # 您可以在这里添加更多用户名

conn = pyodbc.connect('DRIVER={SQL Server};SERVER=localhost;DATABASE=twitter;UID=1;PWD=1')
cursor = conn.cursor()

for user in usernames:
    user_folder = os.path.join(base_path, user)
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

    # 在每个用户的数据处理完成后,对表进行排序
    sort_table_by_time(cursor, table_name)

# 关闭数据库连接
cursor.close()
conn.close()