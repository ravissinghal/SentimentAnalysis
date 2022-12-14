# SentimentAnalysis
Sentiment Analysis of Real Time Data Streamed from Twitter passed through Kafka and Spark


#Prerequisite: 
Twitter Developer Account
Hadoop
Zookeeper
Kafka
Spark
Topic 

twitter_kafka.py: Streams Data from Twitter and send data to the producer of the created Kafka Topic.
spark_model.py: Train a machine learning model on the datatset and save in Hadoop.
kakfa_spark_ML.py: Loads the saved model and then Read data in real time from the Kafka producer and predict the Sentiment of the Tweet.

#Process:
Create Topic: bin/kafka-topics.sh --bootstrap-server localhost:9092 --topic first_topic --create --partitions 3 --replication-factor 1
Start Zookeeper: zookeeper-server-start.sh config/zookeeper.properties
Start Kafka: bin/kafka-server-start.sh config/server.properties
Start Script: python3 twitter_kafka.py
Start Producer: bin/kafka-console-producer.sh --broker-list localhost:9092 --topic first_topic
Start Consumer: bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 
Start Spark: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3 kafka_spark_ML.py   // add jar packages in default conf file so that only spark-submit kafka_spark_ML.py can run.
