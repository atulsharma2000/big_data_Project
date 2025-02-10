import torch
from transformers import pipeline
from pyspark.sql import SparkSession

model_name = "facebook/bart-large-mnli"
device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

def load_model(device):
    """loads the zero-shot classification model."""
    return pipeline("zero-shot-classification", model=model_name, device=device)

news_sentiment_analysis = load_model(device)
sentiment_list = ["positive", "negative", "neutral", "joy", "sadness", "anger", "fear", "trust"]

def analyze_sentiment(article_str):
    """analyzes sentiment of a given article string using zero-shot classification."""
    try:
        analysis_output = news_sentiment_analysis(
            article_str,
            candidate_labels=sentiment_list,
            multi_label=True
        )

        sentiments = {emotion: score for emotion, score in zip(analysis_output['labels'], analysis_output['scores'])}
        return sentiments
    except Exception as e:
        print(f"Error during sentiment analysis: {e}")
        return None

def fetch_news_data(spark, file_path):
    """fetches news data from a JSON file in HDFS using PySpark."""
    try:
        df = spark.read.json(file_path)
        return df
    except Exception as e:
        print(f"Error reading JSON from HDFS: {e}")
        return None

def main():
    """main function to orchestrate news data fetching and sentiment analysis."""
    spark = SparkSession.builder.appName("NewsSentimentAnalysis").getOrCreate()

    # Define the file path for the JSON file in HDFS
    file_path = "hdfs://localhost:9000/user/atul/news_data/sentiment_output/news_data_1_sentiment.json"

    # Fetch news data from HDFS
    news_df = fetch_news_data(spark, file_path)

    if news_df is None:
        print("Failed to fetch news data. Exiting.")
        spark.stop()
        return

    # Collect news articles from DataFrame
    news_articles = news_df.collect()

    for row in news_articles:
        content = row.Summary

        # Analyze sentiment for the article content
        sentiments = analyze_sentiment(content)

        if sentiments:
            print(f"Sentiment analysis for article")
            print(sentiments)
        else:
            print(f"Sentiment analysis failed for article")

    spark.stop()

if __name__ == "__main__":
    main()