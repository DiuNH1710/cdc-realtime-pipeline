# 📡 Realtime CDC Data Pipeline for Recruitment Start-up
🏗️ Giới thiệu dự án

Dự án này mô phỏng một Realtime Change Data Capture (CDC) Pipeline hoàn chỉnh, tương tự hệ thống dữ liệu của một startup tuyển dụng.
Hai tệp dữ liệu tracking.csv và search.csv đóng vai trò nguồn sự kiện giả lập từ client. Các sự kiện này được nạp vào PostgreSQL, sau đó toàn bộ thay đổi được Debezium CDC bắt lại và đẩy vào Kafka dưới dạng stream.

Spark Streaming tiếp nhận dữ liệu từ Kafka, thực hiện bước transform & enrich, rồi ghi kết quả đã xử lý vào MySQL.
Cuối cùng, Grafana sử dụng MySQL làm datasource để trực quan hóa dữ liệu theo thời gian thực (real-time dashboards).

![Capture.PNG](images%2FCapture.PNG)
## 📁 Project Structure
```
realtime-cdc-pipeline-prj2/
│
├── checkpoint/
│   ├── search_offset.txt
│   └── tracking_offset.txt
│
├── csv_files/
│   ├── search.csv
│   ├── tracking.csv
│   └── tracking_clean.csv
│
├── images/
│   └── img.png
│
├── producers/
│   ├── load_search_to_postgres.py
│   └── load_tracking_to_postgres.py
│
├── set-up-mysql-db/
│   └── mysql_schema.sql
│
├── set-up-postgres-db/
│   └── pg_schema.sql
│
├── streams/
│   ├── search_stream.py
│   └── tracking_stream.py
│
├── venv/
│
├── .gitignore
├── clean_csv.py
├── debezium-config.sh
├── docker-compose.yml
├── main.py
├── mysql-connector-j-8.1.0.jar
├── requirements.txt
├── test.py
├── tracking_connector.json
└── README.md

```
### 🧩 1. Kiến trúc tổng quan

Pipeline:

CSV → Producers → PostgreSQL

Debezium CDC theo dõi mọi thay đổi → đẩy vào Kafka

Kafka chứa các CDC topic

Spark Streaming đọc Kafka → transform → ghi vào MySQL

Grafana đọc MySQL → dashboard realtime

### 🐳 2. Khởi chạy toàn bộ services
Trong root project:

```commandline
docker-compose up -d
```


Kiểm tra container:

```commandline
docker ps
```


Bạn sẽ thấy:

![img_1.png](images%2Fimg_1.png)

### 🗄️ 3. Thiết lập PostgreSQL
#### 🧂 Bước 1 — Truy cập container postgres
```commandline
docker exec -it postgres psql -U postgres
```
Check database:
```
\l
```
👉 Nếu không có etl_db, tạo:

```
CREATE DATABASE etl_db;
```
#### 🧂 Bước 2 — Load schema

Truy cập database:

```commandline
docker exec -it postgres psql -U postgres -d etl_db
```

Load file SQL:

```commandline
     i /tmp/pg_schema.sql
```

Nếu file chưa tồn tại trong container → copy:
```commandline
docker cp set-up-postgres-db/pg_schema.sql postgres:/tmp/pg_schema.sql
```
screenshot psql tables
![img_5.PNG](images%2Fimg_5.PNG)

### 🔁 4. Insert CSV vào PostgreSQL
```commandline
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

        time.sleep(0.2)  # delay giả lập realtime
```

Trong local terminal:
```commandline
python producers/load_search_to_postgres.py
python producers/load_tracking_to_postgres.py
```
![img_3.png](images%2Fimg_3.png)
### 👤 5. Tạo user replication cho Debezium
Trong Postgres:
```commandline
docker exec -it postgres psql -U postgres -d etl_db
```
Run:
```commandline
CREATE ROLE debezium WITH LOGIN PASSWORD 'debezium';
ALTER ROLE debezium REPLICATION;
GRANT CONNECT ON DATABASE etl_db TO debezium;
```
### 📣 6. Tạo Publication để Debezium theo dõi bảng
```commandline
CREATE PUBLICATION dbz_pub FOR ALL TABLES;
```
### 🔌 7. Tạo Debezium Connector
Cấu hình debezium:
```commandline
{
  "name": "cdc_tracking_conn",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "plugin.name": "pgoutput",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "debezium",
    "database.password": "debezium",
    "database.dbname": "etl_db",
    "database.server.name": "cdc",
    "publication.name": "dbz_pub",
    "slot.name": "cdc_slot",
    "table.include.list": "public.tracking_events,public.search_by_jobid",
    "topic.prefix": "cdc",
    "tombstones.on.delete": "false",
    "decimal.handling.mode": "string",
    "snapshot.mode": "initial"
  }
}

```

```commandline
$body = Get-Content .\tracking_connector.json -Raw
Invoke-RestMethod -Uri http://localhost:8083/connectors `
  -Method Post -ContentType "application/json" -Body $body
```
![img_2.png](images%2Fimg_2.png)

### 📡 8. Kiểm tra Kafka Topic CDC
Vào Kafka container:

```
docker exec -it kafka bash
```

List topic:
```
kafka-topics --bootstrap-server kafka:29092 --list
```

Bạn sẽ thấy:
```
cdc.public.tracking_events
cdc.public.search_by_jobid
```

Đọc realtime:
```
kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic cdc.public.tracking_events --from-beginning
```
```
kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic cdc.public.search_by_jobid --from-beginning
```
![img_4.png](images%2Fimg_4.png)

### 🔥 9. Chạy Spark Streaming
```commandline
# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("SearchByJobStream") \
    .config("spark.jars", "file:///E:/data-projects/realtime-cdc-pipeline-prj2/mysql-connector-j-8.1.0.jar") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

# Schema cho phần payload.after
schema = StructType([
    StructField("job_id", StringType()),
    StructField("company_name", StringType()),
    StructField("title", StringType()),
    StructField("city_name", StringType()),
    StructField("state", StringType()),
    StructField("major_category", StringType()),
    StructField("minor_category", StringType()),
    StructField("pay_from", StringType()),
    StructField("pay_to", StringType()),
    StructField("pay_type", StringType()),
    StructField("work_schedule", StringType())
    # updated_at bỏ qua, MySQL tự fill
])

# Đọc Kafka
df = (spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "cdc.public.search_by_jobid")
      .option("startingOffsets", "earliest")
      .load())

# Parse JSON: lấy payload.after
df_parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(get_json_object(col("json_str"), "$.payload.after").alias("after_json")) \
    .select(from_json(col("after_json"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("pay_from", col("pay_from").cast("float")) \
    .withColumn("pay_to", col("pay_to").cast("float")) \
    .filter(col("job_id").isNotNull())
```
Trong terminal:

##### Tracking stream
```
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  streams/tracking_stream.py
```
##### Search stream
```
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  streams/search_stream.py
```
Spark logs 
![img_6.PNG](images%2Fimg_6.PNG)

### 🗄️ 10. MySQL (Storage sau transform)

```commandline
def write_to_mysql(batch_df, epoch_id):
    try:
        count = batch_df.count()
        logging.info(f"[Batch {epoch_id}] Rows to write: {count}")
        if count > 0:
            batch_df.show(truncate=False)   # xem dữ liệu thực tế
            batch_df.printSchema()          # xem schema
            batch_df.write \
                .format("jdbc") \
                .mode("append") \
                .option("url", "jdbc:mysql://localhost:3306/etl_db?useSSL=false&serverTimezone=UTC") \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .option("dbtable", "search_by_jobid") \
                .option("user", "root") \
                .option("password", "123456") \
                .save()
            logging.info(f"[Batch {epoch_id}] Written {count} rows to MySQL")
    except Exception as e:
        logging.error(f"[Batch {epoch_id}] Error writing to MySQL: {e}")
```
![img_7.PNG](images%2Fimg_7.PNG)

### 📊 11. Grafana Visualization
1. Mở Grafana:
http://localhost:3000

Login: admin / admin

2. Add data source → MySQL

3. Viết query ví dụ:
```
SELECT minor_category, COUNT(*) AS job_count
FROM search_by_jobid
GROUP BY minor_category
ORDER BY job_count DESC;

```
![img_8.png](images%2Fimg_8.png)