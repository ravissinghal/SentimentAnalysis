from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.linalg import DenseVector
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import CountVectorizerModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


# Spark set-up
conf = SparkConf()
conf.setAppName("Classifier")

sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

spark = SparkSession(sc)

method = 'LR'

# Load dataset file as RDD
rdd = sc.textFile("/user/spark/SentimentAnalysisDataset3.txt")
rdd = rdd.map(lambda x: x.split(','))

rdd = rdd.map(lambda x: [int(x[0]), int(x[1]), str(x[2]), str(x[3])]) 

# Create dataframe for ML model
df = spark.createDataFrame(rdd, ["ItemID", "Sentiment", "SentimentSource", "SentimentText"])
#df.show(5)

tokenizer = Tokenizer(inputCol="SentimentText", outputCol="words")
tokenized = tokenizer.transform(df)
#tokenized.show(1, truncate=False)

remover = StopWordsRemover(inputCol="words", outputCol="filtered", stopWords=["I", "the", "had", "a", ".", " ", "is", "are", "am", "has", "had", '"', "'", "@"])
filtered = remover.transform(tokenized)
#filtered.show(1, truncate=False)

cv = CountVectorizer(inputCol="filtered", outputCol="vector")
model1 = cv.fit(filtered)
result = model1.transform(filtered)
#result.show(1, truncate=False)

trial = result.select("vector", "Sentiment")

# Declare ML model
if method == 'LR':
	clf = LogisticRegression(featuresCol = "vector", labelCol = "Sentiment", maxIter=100, regParam=0.1)
elif method == 'NB':
	clf = NaiveBayes(featuresCol = "vector", labelCol = "Sentiment", smoothing=1.0, modelType="gaussian")

# Train the model using training data
model2 = clf.fit(trial)

import os, tempfile
#path = tempfile.mkdtemp()
path1 = 'user/cVector'
path2 = 'user/model'

model1.write().overwrite().save(path1)
model2.write().overwrite().save(path2)

#test1 = CounterVectorizerModel.load(path1)
#test2 = LogisticRegressionModel.load(path2)
