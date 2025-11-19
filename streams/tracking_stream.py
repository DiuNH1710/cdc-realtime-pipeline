# streams/tracking_stream.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, get_json_object
from pyspark.sql.types import StructType, StructField, StringType
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("TrackingEventsStream") \
    .config("spark.jars", "file:///E:/data-projects/realtime-cdc-pipeline-prj2/mysql-connector-j-8.1.0.jar") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

# Schema cho payload.after
schema = StructType([
    StructField("uuid", StringType()),
    StructField("create_time", StringType()),
    StructField("job_id", StringType()),
    StructField("custom_track", StringType()),
    StructField("bid", StringType()),
    StructField("campaign_id", StringType()),
    StructField("group_id", StringType()),
    StructField("publisher_id", StringType()),
    StructField("ev", StringType()),
    StructField("ts", StringType())
])

# Đọc Kafka
df = (spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "cdc.public.tracking_events")
      .option("startingOffsets", "earliest")
      .load())

# Parse JSON: lấy payload.after
df_parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(get_json_object(col("json_str"), "$.payload.after").alias("after_json")) \
    .select(from_json(col("after_json"), schema).alias("data")) \
    .select("data.*") \
    .filter(col("uuid").isNotNull())

# Hàm ghi xuống MySQL
def write_to_mysql(batch_df, epoch_id):
    try:
        count = batch_df.count()
        logging.info(f"[Batch {epoch_id}] Rows to write: {count}")
        if count > 0:
            batch_df.show(truncate=False)   # log dữ liệu thực tế
            batch_df.printSchema()
            batch_df.write \
                .format("jdbc") \
                .mode("append") \
                .option("url", "jdbc:mysql://localhost:3306/etl_db?useSSL=false&serverTimezone=UTC") \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .option("dbtable", "tracking_events") \
                .option("user", "root") \
                .option("password", "123456") \
                .save()
            logging.info(f"[Batch {epoch_id}] Written {count} rows to MySQL")
    except Exception as e:
        logging.error(f"[Batch {epoch_id}] Error writing to MySQL: {e}")

# Khởi động streaming query
query = df_parsed.writeStream \
    .foreachBatch(write_to_mysql) \
    .outputMode("append") \
    .start()

logging.info("Tracking stream started...")
query.awaitTermination()
