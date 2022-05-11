
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DoubleType, StructType, StructField, TimestampType, IntegerType
from pyspark.sql.functions import col


# create schema
schema = StructType([ 
    StructField("VendorID",StringType(), True), 
    StructField("tpep_pickup_datetime",TimestampType(), True), 
    StructField("tpep_dropoff_datetime",TimestampType(), True), 
    StructField("passenger_count", IntegerType(), True), 
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", IntegerType(), True), 
    StructField("store_and_fwd_flag", StringType(), True), 
    StructField("PULocationID", IntegerType(), True), 
    StructField("DOLocationID", IntegerType(), True), 
    StructField("payment_type", IntegerType(), True), 
    StructField("fare_amount", DoubleType(), True), 
    StructField("extra", DoubleType(), True),  
    StructField("mta_tax", DoubleType(), True), 
    StructField("tip_amount", DoubleType(), True), 
    StructField("tolls_amount", DoubleType(), True), 
    StructField("improvement_surcharge", DoubleType(), True), 
    StructField("total_amount", DoubleType(), True), 
    StructField("congestion_surcharge", DoubleType(), True)
  ])

spark = SparkSession.builder.appName('Taxi')\
        .config('spark.jars.packages', 'org.postgresql:postgresql:42.2.14')\
        .getOrCreate()

path = "hdfs://192.168.64.14:9000/raws/"
df_taxi = spark.read.option("header", True).csv(path=path, schema=schema)
df_taxi.show(5)
df_taxi.printSchema()

df_taxi = df_taxi.where(col("VendorID").isNotNull())

#write to postgres
df_taxi.write \
    .mode("overwrite") \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/Test")\
    .option("dbtable", "taxitrip") \
    .option("user", "postgres") \
    .option("password", "Huy12345678") \
    .option("driver", "org.postgresql.Driver") \
    .save()

spark.stop()




