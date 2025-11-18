import pandas as pd
import psycopg2
from datetime import datetime
import os
import time

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

# ---------- HELPER TO HANDLE NaN -------------
def none_if_nan(val):
    if pd.isna(val):
        return None
    return str(val)

# ---------- INSERT SEARCH.CSV -------------
def load_search_csv(file_path, offset_file):
    df = pd.read_csv(file_path, header=0)
    # loại bỏ cột index thừa và reset index
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df.reset_index(drop=True, inplace=True)
    columns = df.columns.tolist()

    last_offset = read_offset(offset_file)
    new_rows = df.iloc[last_offset:]
    print(f"[SEARCH] Inserting {len(new_rows)} new rows starting from offset {last_offset}...")

    for idx, row in new_rows.iterrows():
        values = [none_if_nan(row[col]) for col in columns]
        values.append(datetime.now())  # updated_at timestamp

        placeholders = ','.join(['%s'] * len(values))
        sql = f"""
        INSERT INTO search_by_jobid ({','.join(columns)}, updated_at)
        VALUES ({placeholders})
        ON CONFLICT (job_id) DO NOTHING
        """
        cursor.execute(sql, tuple(values))

        print(f"[SEARCH] Inserted row {idx + 1}: job_id={row['job_id']}")
        write_offset(offset_file, idx + 1)  # checkpoint = index + 1

        time.sleep(1)  # delay giả lập realtime

# ---------- MAIN -------------
if __name__ == "__main__":
    load_search_csv("csv_files/search.csv", "checkpoint/search_offset.txt")
    cursor.close()
    conn.close()
