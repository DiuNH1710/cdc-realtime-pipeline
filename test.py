# test_spark_mysql.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import logging
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

spark = SparkSession.builder \
    .appName("TestMySQLWrite") \
    .config("spark.jars", "file:///E:/data-projects/realtime-cdc-pipeline-prj2/mysql-connector-j-8.1.0.jar") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.4")\
    .getOrCreate()

# Tạo DataFrame giả lập
data = [
    ("1", "Alice", 100.5),
    ("2", "Bob", 200.0),
    ("3", "Charlie", 300.25)
]
columns = ["id", "name", "value"]

df = spark.createDataFrame(data, columns)

# Viết xuống MySQL bằng JDBC
def write_to_mysql(batch_df, epoch_id):
    count = batch_df.count()
    logging.info(f"[Batch {epoch_id}] Rows to write: {count}")
    if count > 0:
        batch_df.write \
            .format("jdbc") \
            .mode("append") \
            .option("url", "jdbc:mysql://localhost:3306/etl_db") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", "test_stream") \
            .option("user", "root") \
            .option("password", "123456") \
            .save()
        logging.info(f"[Batch {epoch_id}] Written {count} rows to MySQL")

# Mô phỏng foreachBatch (1 batch)
write_to_mysql(df, 0)

logging.info("Test finished")
