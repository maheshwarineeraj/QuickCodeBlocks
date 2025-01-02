"""
Details - https://medium.com/towards-data-engineering/integrating-apache-spark-with-redis-for-low-latency-data-access-37ff1a1761ec 

Workflow:
 1. Read Data: Load data into Spark from source systems (dummy data in this case).
 2. Transform Data: Clean and preprocess the data.
 3. Partition: Repartition the DataFrame for parallelism. (performance optimization)
 4. Write to Redis: Use Spark tasks to write data in parallel.

"""

# create spark session
spark = SparkSession \
        .builder \
        .appName("Spark Redis Integration") \
        .getOrCreate()


# create dummy dataframe
df = spark.createDataFrame([[1, "abc", 5, "2024-05-09"],
             [2, "def", 36, "2024-05-11"],
             [3, "ghi", 2, "2024-05-05"],
             [4, "jkl", 26, "2024-06-01"],
             [5, "mno", 75, "2024-06-09"]],
             schema=["user_id", "name", "visit_count", "last_visit_date"])

# write spark dataframe to Redis
# table is equivalent prefix in Redis
df.write.format("org.apache.spark.sql.redis") \
    .option("table", "user") \ # Redis Prefix
    .option("key.column", "user_id") \ # Unique Key
    .option("host", "localhost") \
    .option("port", "6379").save()
