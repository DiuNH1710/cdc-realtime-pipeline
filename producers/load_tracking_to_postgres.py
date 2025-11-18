import pandas as pd
import psycopg2
from datetime import datetime
import os
import time
from dateutil import parser
import uuid

# ---------- CONNECT POSTGRES -------------
conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="etl_db",
    user="postgres",
    password="123456"
)
conn.autocommit = True
cursor = conn.cursor()

# ---------- CHECKPOINT -------------
def read_offset(path):
    if not os.path.exists(path):
        return 0
    with open(path, "r") as f:
        return int(f.read().strip() or 0)

def write_offset(path, value):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(str(value))

# ---------- HELPER TO HANDLE NaN & DATETIME -------------
def safe_value(val):
    if val is None or pd.isna(val):
        return None
    return str(val)

def parse_datetime(val):
    if val is None or pd.isna(val) or val == '':
        return None
    try:
        return parser.parse(val)
    except:
        return None

# ---------- INSERT TRACKING.CSV -------------
def load_tracking_csv(file_path, offset_file):
    # Các cột giữ nguyên từ CSV
    columns = [
        'create_time','bid','bn','campaign_id','cd','custom_track','de','dl','dt','ed',
        'ev','group_id','id','job_id','md','publisher_id','rl','sr','ts','tz','ua','uid',
        'utm_campaign','utm_content','utm_medium','utm_source','utm_term','v','vp'
    ]

    df = pd.read_csv(file_path, header=0)
    # loại bỏ cột rác / Unnamed
    df = df.loc[:, df.columns.str.strip() != '']
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df = df.reset_index(drop=True)
    df = df[[col for col in columns if col in df.columns]]

    last_offset = read_offset(offset_file)
    new_rows = df.iloc[last_offset:]
    print(f"[TRACKING] Inserting {len(new_rows)} new events starting from offset {last_offset}...")

    for idx, row in new_rows.iterrows():
        row_dict = row.to_dict()

        # parse datetime
        if 'create_time' in row_dict:
            row_dict['create_time'] = parse_datetime(row_dict['create_time'])

        # tạo uuid làm PRIMARY KEY
        uuid_key = str(uuid.uuid4())

        # đảm bảo tất cả giá trị là string
        values = [safe_value(row_dict.get(col)) for col in columns]
        values.insert(0, uuid_key)  # uuid là cột đầu tiên
        values.append(datetime.now())  # updated_at

        placeholders = ','.join(['%s'] * len(values))
        sql = f"""
        INSERT INTO tracking_events (
            uuid, {','.join(columns)}, updated_at
        ) VALUES ({placeholders})
        """

        cursor.execute(sql, tuple(values))
        print(f"[TRACKING] Inserted row {idx + 1}: uuid={uuid_key}")
        write_offset(offset_file, idx + 1)  # checkpoint = index + 1

        time.sleep(1)  # delay 1s để giả lập realtime

# ---------- MAIN -------------
if __name__ == "__main__":
    load_tracking_csv("csv_files/tracking.csv", "checkpoint/tracking_offset.txt")
    cursor.close()
    conn.close()
