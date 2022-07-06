
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DoubleType, StructType, StructField, TimestampType, IntegerType
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans


# create schema
schema = StructType([ 
    StructField("VendorID",StringType(), True), 
    StructField("tpep_pickup_datetime",TimestampType(), True), 
    StructField("tpep_dropoff_datetime",TimestampType(), True), 
    StructField("passenger_count", IntegerType(), True), 
    StructField("trip_distance", DoubleType(), True),
     StructField("pickup_longitude", DoubleType(), True),
    StructField("pickup_latitude", DoubleType(), True),
    StructField("RatecodeID", IntegerType(), True), 
    StructField("store_and_fwd_flag", StringType(), True), 
    StructField("dropoff_longitude", DoubleType(), True), 
    StructField("dropoff_latitude", DoubleType(), True), 
    StructField("payment_type", IntegerType(), True), 
    StructField("fare_amount", DoubleType(), True), 
    StructField("extra", DoubleType(), True),  
    StructField("mta_tax", DoubleType(), True), 
    StructField("tip_amount", DoubleType(), True), 
    StructField("tolls_amount", DoubleType(), True), 
    StructField("improvement_surcharge", DoubleType(), True), 
    StructField("total_amount", DoubleType(), True),
  ])

spark = SparkSession.builder.appName('Taxi')\
        .config('spark.jars.packages', 'org.postgresql:postgresql:42.2.14')\
        .getOrCreate()

path = "hdfs://192.168.64.14:9000/raws/"
df_taxi = spark.read.option("header", True).csv(path=path, schema=schema)
df_taxi.show(5)
df_taxi.printSchema()

# Denfine features vector to use for kmeans algorithm
featureCols = ['pickup_latitude', 'pickup_longitude']
assembler = VectorAssembler(inputCols=featureCols, outputCol='features')

df_taxi2 = assembler.transform(df_taxi)
df_taxi2.show(5)

# setK(20) phân thành 20 cụm
# setFeaturesCol("features") dùng để train
# setPredictionCol("cid") dùng để predict
kmeans = KMeans().setK(20).setFeaturesCol("features").setPredictionCol("cid").setSeed(1)
model = kmeans.fit(df_taxi2)

# Shows the result 20 cluster.
centers = model.clusterCenters()
i=0
print("Cluster Centers: ")
for center in centers:
    print(i, center)
    i += 1

# make prediction
df_predicted = model.transform(df_taxi2)

df_taxi_locates = df_predicted.drop(df_predicted.features)
df_taxi_locates.show(5)

#write to postgres
df_taxi_locates.write \
    .mode("overwrite") \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/Test")\
    .option("dbtable", "taxitrip") \
    .option("user", "postgres") \
    .option("password", "Huy12345678") \
    .option("driver", "org.postgresql.Driver") \
    .save()

spark.stop()




