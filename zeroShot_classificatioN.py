from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lower, regexp_replace
from pyspark.sql.types import FloatType
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

# Initialize SparkSession
spark = SparkSession.builder.appName("BatchSentimentAnalysis").getOrCreate()

# Download VADER lexicon if you haven't already
try:
    sid = SentimentIntensityAnalyzer()
except LookupError:
    nltk.download('vader_lexicon')
    sid = SentimentIntensityAnalyzer()

# Define the sentiment analysis function
def analyze_sentiment(text):
    if not text:
        return 0.0
    scores = sid.polarity_scores(text)
    return scores['compound']

# register the function as a UDF
analyze_sentiment_udf = udf(analyze_sentiment, FloatType())

# input and output paths
base_input_path = "hdfs://localhost:9000/user/atul/news_data/input/"
output_csv_path = "hdfs://localhost:9000/user/atul/news_data/zero_shot_output/sentiment_summary.csv"  

num_files = 871  # total number of files
all_data = []  # To collect all DataFrames

for i in range(1, num_files + 1):
    # Construct the file path
    input_file_path = f"{base_input_path}news_data_{i}.json"

    try:
        # Read the JSON file
        df = spark.read.json(input_file_path)

        # Check if the DataFrame is empty
        if df.count() == 0:
            print(f"Skipping empty file: {input_file_path}")
            continue

        # Preprocess and perform sentiment analysis (assuming a 'text' or 'Summary' column)
        # Adapt this to your actual column name
        text_column = "Summary"  # Or "Content" or whatever column holds the text

        # Check if the text_column exists
        if text_column not in df.columns:
            print(f"Skipping {input_file_path} as column {text_column} does not exist.")
            continue

        df = df.withColumn(text_column, lower(col(text_column)))
        df = df.withColumn(text_column, regexp_replace(col(text_column), "[^a-zA-Z0-9\\s]", ""))
        df = df.withColumn("sentiment_score", analyze_sentiment_udf(col(text_column)))

        # Select only the desired columns
        df_selected = df.select("Summary", "sentiment_score")

        all_data.append(df_selected)  # Collect the DataFrames
        print(f"Successfully processed: {input_file_path}")

    except Exception as e:
        print(f"Error processing {input_file_path}: {e}")

# Union all the DataFrames
if all_data:
    final_df = all_data[0]
    for df in all_data[1:]:
        final_df = final_df.union(df)


    # Write the combined DataFrame to a single CSV file
    final_df.coalesce(1).write.option("header", "true").mode("overwrite").csv(output_csv_path)
    print(f"Successfully combined and saved all data to: {output_csv_path}")
else:
    print("No data was processed.")

# Stop the SparkSession
spark.stop()
