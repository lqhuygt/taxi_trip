# Build data pipeline analysis of popular Taxi Trip

### 1. Simulate FTP server
- Create python file to simulate ftp server ingest with Kafka
- When new data coming systems will automate scan in ftp server
- Create bash script file to create data

### 2. Streaming data
**Using Spark Structure Streaming + Apache Kafka + Apache Airflow to buid pipeline**
- add some packages to talk to Kafka.
  - spark-streaming-kafka-0-10_2.12:3.1.1
  - spark-sql-kafka-0-10_2.12:3.1.1
- Ingest data from ftp server to Kafka
- Read stream data to Stream topic
- Read batch data from Stream topic to Batch topic
- Transfomation data
- Write stream data to HDFS
- Schedule and monitor pipeline with Airflow

### 3. Cluster model
**Using SparkML to train model**
- Read data csv file from Hadoop
- Create "features" column to train and clustering
- Save data trained by model to PostgreSQL

### 4. Analyst data
**Using Tableau to analyze and visualize data**
- Which clusters had the highest number of pickups?
- Which hours of the day had the highest number of pickups?
- Which days of week had the highest number of pickups?
- What is the difference between tip money of credit card and cash payment
https://public.tableau.com/app/profile/huyle3564/viz/TaxiTripdynamic2/TaxiTripOverview3
