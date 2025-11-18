import csv
import psycopg2
from datetime import datetime
import uuid
import os
import time
from dateutil import parser

# kết nối Postgres
conn = psycopg2.connect(
    host="localhost", port=5433, database="etl_db", user="postgres", password="123456"
)
conn.autocommit = True
cursor = conn.cursor()

# checkpoint
def read_offset(path):
    if not os.path.exists(path):
        return 0
    with open(path) as f:
        return int(f.read().strip() or 0)

def write_offset(path, value):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(str(value))

# helper
def parse_datetime(val):
    if val is None or val == '':
        return None
    try:
        return parser.parse(val)
    except:
        return None

def none_if_empty(val):
    if val is None or val.strip() == '':
        return None
    return val.strip()

# insert tracking
def load_tracking_csv(file_path, offset_file):
    columns = [
        'create_time','bid','bn','campaign_id','cd','custom_track','de','dl','dt','ed',
        'ev','group_id','id','job_id','md','publisher_id','rl','sr','ts','tz','ua','uid',
        'utm_campaign','utm_content','utm_medium','utm_source','utm_term','v','vp'
    ]

    last_offset = read_offset(offset_file)

    with open(file_path, encoding='utf-8') as f:
        raw_lines = f.readlines()
        clean_lines = []

        for raw in raw_lines:
            line = raw.strip()

            # 1. bỏ ký tự " ở đầu dòng
            if line.startswith('"'):
                line = line[1:]

            # 2. bỏ dấu ;;; hoặc ;; ở cuối
            while line.endswith(';'):
                line = line[:-1]

            clean_lines.append(line)

        reader = csv.DictReader(clean_lines)

        rows = list(reader)[last_offset:]
        print(f"[TRACKING] Inserting {len(rows)} new rows starting from offset {last_offset}...")

        for idx, row in enumerate(rows):
            uuid_key = str(uuid.uuid4())
            values = []

            for col in columns:
                val = row.get(col)
                if col == 'create_time':
                    values.append(parse_datetime(val))
                else:
                    values.append(none_if_empty(val))

            values.insert(0, uuid_key)
            values.append(datetime.now())

            placeholders = ','.join(['%s'] * len(values))
            sql = f"INSERT INTO tracking_events (uuid, {','.join(columns)}, updated_at) VALUES ({placeholders})"

            try:
                cursor.execute(sql, tuple(values))
            except Exception as e:
                print(f"[ERROR] Row {last_offset + idx + 1}: {e}")
                continue

            write_offset(offset_file, last_offset + idx + 1)
            print(f"[TRACKING] Inserted row {last_offset + idx + 1}: uuid={uuid_key}")
            time.sleep(1)
# main
if __name__ == "__main__":
    load_tracking_csv("csv_files/tracking.csv", "checkpoint/tracking_offset.txt")
    cursor.close()
    conn.close()
