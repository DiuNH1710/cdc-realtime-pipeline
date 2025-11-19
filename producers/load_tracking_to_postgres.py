from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import psycopg2
import uuid
import os
import time
from datetime import datetime

# =============================
# POSTGRES CONNECTION
# =============================
conn = psycopg2.connect(
    host="localhost", port=5433, database="etl_db", user="postgres", password="123456"
)
conn.autocommit = True
cursor = conn.cursor()

# =============================
# CHECKPOINT
# =============================
def read_offset(path):
    if not os.path.exists(path):
        return 0
    with open(path) as f:
        return int(f.read().strip() or 0)

def write_offset(path, value):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(str(value))

# =============================
# CLEAN HELPERS
# =============================
def clean_str(val):
    if val is None:
        return None
    val = str(val).strip()
    return val if val != "" else None

# =============================
# MAIN LOAD FUNCTION
# =============================
def load_tracking_csv_spark(csv_path, offset_file):

    spark = (
        SparkSession.builder
        .appName("TrackingCSVLoader")
        .master("local[*]")
        .getOrCreate()
    )

    # Read with | delimiter and disable quote/escape
    df = (
        spark.read
        .option("header", True)
        .option("delimiter", "|")
        .option("quote", "\u0000")   # disable quotes
        .option("escape", "\u0000")  # disable escaping
        .csv(csv_path)
    )

    # Fix header vp; -> vp if present
    cols = df.columns
    fixed_cols = [c.rstrip(";") for c in cols]
    if fixed_cols != cols:
        df = df.toDF(*fixed_cols)

    # Trim leading/trailing quotes from all string columns
    for c in df.columns:
        df = df.withColumn(
            c,
            F.when(F.col(c).isNotNull(),
                   F.regexp_replace(F.regexp_replace(F.col(c), r'^"', ''), r'"$', '')
            ).otherwise(F.col(c))
        )

    total = df.count()
    print(f"\n🔵 Spark loaded {total} rows\n")
    df.printSchema()
    df.show(5, truncate=False)

    last_offset = read_offset(offset_file)
    print(f"\n🟡 Starting from offset: {last_offset}\n")

    rows = df.collect()

    columns = [
        'create_time','bid','bn','campaign_id','cd','custom_track','de','dl','dt','ed',
        'ev','group_id','id','job_id','md','publisher_id','rl','sr','ts','tz','ua','uid',
        'utm_campaign','utm_content','utm_medium','utm_source','utm_term','v','vp'
    ]

    for idx, row in enumerate(rows[last_offset:], start=last_offset):
        print("\n===== 🔹 SPARK ROW 🔹 =====")
        print(row.asDict())
        print("===========================\n")

        uuid_key = str(uuid.uuid4())
        values = []

        for col_name in columns:
            val = row[col_name] if col_name in row else None
            values.append(clean_str(val))

        # Add uuid at beginning + timestamp at end
        values.insert(0, uuid_key)
        values.append(datetime.now().isoformat())

        placeholders = ",".join(["%s"] * len(values))
        sql = f"""
            INSERT INTO tracking_events (
                uuid, {",".join(columns)}, updated_at
            ) VALUES ({placeholders})
        """

        try:
            cursor.execute(sql, tuple(values))
            print(f"✅ Inserted row {idx} uuid={uuid_key}")
        except Exception as e:
            print(f"❌ ERROR row {idx}: {e}")
            continue

        write_offset(offset_file, idx + 1)
        time.sleep(2)

    print("\n🎉 DONE LOADING CSV with Spark!\n")
    spark.stop()

# =============================
# RUN
# =============================
if __name__ == "__main__":
    load_tracking_csv_spark("csv_files/tracking_clean.csv", "checkpoint/tracking_offset.txt")
    cursor.close()
    conn.close()
