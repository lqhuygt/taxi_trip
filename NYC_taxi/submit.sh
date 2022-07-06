PYSPARK_PYTHON=/home/hadoopuser/anaconda3/envs/pyspark_conda_env/bin/python spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 --total-executor-cores 3 --executor-cores 1 --executor-memory 1000M --num-executors 2 --master yarn --driver-memory 512M --archives /home/hadoopuser/pyspark_conda_env.tar.gz DetectionWebAttackStreaming.py




spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 --total-executor-cores 2 --executor-cores 1 --executor-memory 1000M --num-executors 2 --master yarn --driver-memory 512M /home/hadoopuser/NYC_taxi/stream_taxi.py