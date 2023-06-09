from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, window, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, TimestampType
from datetime import datetime, timedelta

# Define schema for reading from Cassandra
schema = StructType([
    StructField("page_id", IntegerType()),
    StructField("title", StringType()),
    StructField("domain", StringType()),
    StructField("user_id", IntegerType()),
    StructField("create_time", TimestampType()),
    StructField("created_by_bot", BooleanType())
])

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("WikiStreamProcessor") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .getOrCreate()

# Read data from Cassandra
df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="pages", keyspace="wiki_stream") \
    .load()

# Get start and end time for statistics computation
end_time = datetime.now() - timedelta(hours=1)  # Excluding the last hour
start_time = end_time - timedelta(hours=6)  # Last 6 hours

# Filter data based on start_time and end_time
df = df.filter((df.create_time > start_time) & (df.create_time <= end_time))

# Compute user activity
user_activity = df.groupBy("user_id").agg(count("*").alias("page_count"))

# Write user activity to PostgreSQL
user_activity.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/postgres") \
    .option("dbtable", "user_activity") \
    .option("user", "postgres") \
    .option("password", "password") \
    .mode('overwrite') \
    .save()

# Compute top users
top_users = df.groupBy("user_id").agg(count("*").alias("page_count")).orderBy(col("page_count").desc()).limit(20)

# Write top users to PostgreSQL
top_users.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/postgres") \
    .option("dbtable", "top_users") \
    .option("user", "postgres") \
    .option("password", "password") \
    .mode('overwrite') \
    .save()

# Stop the spark session
spark.stop()
