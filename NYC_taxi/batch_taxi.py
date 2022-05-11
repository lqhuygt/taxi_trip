from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructType, StructField


# create spark
spark = SparkSession.builder.appName('Batch')\
    .config('spark.jars.packages', 'org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2')\
    .getOrCreate()

# read stream from kafka
df_taxi_batch = spark \
      .read \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "localhost:9092") \
      .option("subscribe", "BatchTaxi") \
      .option("startingOffsets", "earliest") \
      .load()
df_taxi_batch.printSchema()

# create schema
schema = StructType([ 
    StructField("VendorID",StringType(), True), 
    StructField("tpep_pickup_datetime",StringType(), True), 
    StructField("tpep_dropoff_datetime",StringType(), True), 
    StructField("passenger_count", StringType(), True), 
    StructField("trip_distance", StringType(), True),
    StructField("RatecodeID", StringType(), True), 
    StructField("store_and_fwd_flag", StringType(), True), 
    StructField("PULocationID", StringType(), True), 
    StructField("DOLocationID", StringType(), True), 
    StructField("payment_type", StringType(), True), 
    StructField("fare_amount", StringType(), True), 
    StructField("extra", StringType(), True),  
    StructField("mta_tax", StringType(), True), 
    StructField("tip_amount", StringType(), True), 
    StructField("tolls_amount", StringType(), True), 
    StructField("improvement_surcharge", StringType(), True), 
    StructField("total_amount", StringType(), True), 
    StructField("congestion_surcharge", StringType(), True)
  ])

# Parsing the messeage value into dataframe
df_taxi_batch_cast = df_taxi_batch.select(from_json(col("value").cast("string"), schema).alias("value"))
df_taxi_batch_casted = df_taxi_batch_cast.selectExpr("value.VendorID", "value.tpep_pickup_datetime", "value.tpep_dropoff_datetime", 
        "value.passenger_count", "value.trip_distance", "value.RatecodeID", "value.store_and_fwd_flag", "value.PULocationID", "value.DOLocationID",
         "value.payment_type", "value.fare_amount", "value.extra", "value.mta_tax", "value.tip_amount", "value.tolls_amount", 
         "value.improvement_surcharge", "value.total_amount", "value.congestion_surcharge")
df_taxi_batch_casted.printSchema()
df_taxi_batch_casted.show(5)

path = "hdfs://192.168.64.14:9000/raws/"

# write to hdfs
push_to_hdfs = df_taxi_batch_casted.write \
                    .mode("overwrite") \
                    .format("csv") \
                    .save(path)

spark.stop()



