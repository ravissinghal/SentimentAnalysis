from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time
from pyspark.ml.linalg import DenseVector
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import CountVectorizerModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

kafka_topic_name = "first_topic"
kafka_bootstrap_server = 'localhost:9092'
spark = SparkSession.builder.appName("Pyspark").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

cVector = CountVectorizerModel.load('cVector')
tModel = LogisticRegressionModel.load('model')

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers",kafka_bootstrap_server).option("subscribe",kafka_topic_name).option("straightOffsets","latest").load()

print("Printing schema of df: ")
df.printSchema()

df1 = df.selectExpr("CAST(value AS STRING)", "timestamp")

m_schema = "Text STRING"

df2 = df1.select(from_csv(col("value"), m_schema).alias("Tweets"))

df3 = df2.select("Tweets.*")
cols = ['Text','prediction']

df3.printSchema()


tokenizer = Tokenizer(inputCol="Text", outputCol="words")
tokenized = tokenizer.transform(df3)

remover = StopWordsRemover(inputCol="words", outputCol="filtered", stopWords=["I", "the", "had", "a", ".", " ", "is", "are", "am", "has", "had", '"', "'", "@","RT"])
filtered = remover.transform(tokenized)

df4 = cVector.transform(filtered)

df5 = df4.select("vector")

df6 = tModel.transform(df5)

df7 = df6.select("prediction")

df_write_stream = df3.writeStream.trigger(processingTime='2 seconds').outputMode("update").option("truncate","false").format("console").start()

df_write_stream1 = df7.writeStream.trigger(processingTime='2 seconds').outputMode("update").option("truncate","false").format("console").start()

df_write_stream.awaitTermination()
df_write_stream1.awaitTermination()

print("End")
