import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace
from pyspark.ml.feature import Tokenizer, StopWordsRemover

spark = SparkSession.builder\
    .appName("data_clean_hdfs") \
    .getOrCreate()

filePath = "hdfs://localhost:9000/user/atul/news_data/input/news_data_1.json"

df = spark.read.json(filePath)
df = df.dropDuplicates()
df = df.dropna()


df = df.withColumn("Content", regexp_replace(col("Content"), "[^a-zA-Z0-9\\s]", ""))
df = df.withColumn("Content", lower(col("Content")))

df = df.withColumn("Summary", regexp_replace(col("Summary"), "[^a-zA-Z0-9\\s]", ""))
df = df.withColumn("Summary", lower(col("Summary")))


tokenizer = Tokenizer(inputCol="Content", outputCol="Content_words")
wordsData = tokenizer.transform(df)

tokenizer = Tokenizer(inputCol="Summary", outputCol="Summary_words")
wordsData = tokenizer.transform(wordsData)

token_df = wordsData.select('Content_words','Summary_words')

# Stop Word Removal
stopwords = StopWordsRemover.loadDefaultStopWords("english")
custom_stopwords = ["example", "another","cnn","","-","--","–","(cnn)"]
all_stopwords = stopwords + custom_stopwords

remover = StopWordsRemover(inputCol="Content_words", outputCol="Content_filtered", stopWords=all_stopwords)
new_filteredData = remover.transform(token_df)


remover = StopWordsRemover(inputCol="Summary_words", outputCol="Summary_filtered", stopWords=all_stopwords)
new_filteredData = remover.transform(new_filteredData)

# explode the 'filtered' column to count individual words
word_counts = new_filteredData.select(explode(col("Content_filtered")).alias("word")) \
                           .groupBy("word") \
                           .count() \
                           .orderBy(col("count").desc())
                   
# in case to see the most frequent words
# print("Most frequent words after initial stop word removal:")
# word_counts.show(truncate=False)
