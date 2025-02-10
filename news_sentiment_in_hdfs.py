from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lower, regexp_replace
from pyspark.sql.types import FloatType
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
import os

spark = SparkSession.builder.appName("BatchSentimentAnalysis").getOrCreate()

# download VADER lexicon
try:
    sid = SentimentIntensityAnalyzer()
except LookupError:
    nltk.download('vader_lexicon')
    sid = SentimentIntensityAnalyzer()

# Define the sentiment analysis function (same as before)
def analyze_sentiment(text):
    if not text:
        return 0.0
    scores = sid.polarity_scores(text)
    return scores['compound']

# register the function as a UDF
#  means that we are taking a regular Python function, analyze_sentiment, 
# and wrapping it so that it can be used within a PySpark DataFrame transformation
# this line of code prepares our Python sentiment analysis logic to be executed on a distributed dataset within PySpark. 
# It bridges the gap between Python code and the Spark SQL engine. 
# The resulting analyze_sentiment_udf can then be used with select() or withColumn() to apply the sentiment analysis to a DataFrame.
analyze_sentiment_udf = udf(analyze_sentiment, FloatType())


# filePath = "hdfs://localhost:9000/user/atul/news_data/input/news_data_1.json"
base_input_path = "hdfs://localhost:9000/user/atul/news_data/input/"
base_output_path = "hdfs://localhost:9000/user/atul/news_data/sentiment_output/"

num_files = 871  # Total number of files

for i in range(1, num_files + 1):
    # constructing the file paths
    input_file_path = f"{base_input_path}news_data_{i}.json"
    output_file_path = f"{base_output_path}news_data_{i}_sentiment.json"

    try:
        # Read the JSON file
        df = spark.read.json(input_file_path)

        # checking if the DataFrame is empty
        if df.count() == 0:
            print(f"Skipping empty file: {input_file_path}")
            continue

        # preprocess and perform sentiment analysis
        text_column = "Summary"  
        
        # checking if the text_column exists
        if text_column not in df.columns:
            print(f"Skipping {input_file_path} as column {text_column} does not exist.")
            continue
        df = df.withColumn(text_column, lower(col(text_column)))
        df = df.withColumn(text_column, regexp_replace(col(text_column), "[^a-zA-Z0-9\\s]", ""))
        df = df.withColumn("sentiment_score", analyze_sentiment_udf(col(text_column)))

        # selectign only the desired columns
        df_selected = df.select("Summary", "sentiment_score")

        # write the DataFrame to the output path
        df_selected.write.mode("overwrite").json(output_file_path)

        print(f"Successfully processed and saved: {input_file_path} to {output_file_path}")

    except Exception as e:
        print(f"Error processing {input_file_path}: {e}")


spark.stop()