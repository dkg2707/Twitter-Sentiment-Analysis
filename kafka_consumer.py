import nltk
from kafka import KafkaConsumer
import json
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from elasticsearch import Elasticsearch
from textblob import TextBlob

analyser = SentimentIntensityAnalyzer()
elastic = Elasticsearch()


def main():
    # set-up a Kafka consumer
    kafka_consumer = KafkaConsumer("twitter_sentiment")
    for msg in kafka_consumer:
        msg_data = json.loads(msg.value)
        tweet = TextBlob(msg_data["text"])
        sentiment_score = analyser.polarity_scores(tweet)
        negative_score = sentiment_score['neg']
        neutral_score = sentiment_score['neu']
        positive_score = sentiment_score['pos']
        compound_score = sentiment_score['compound']
        if compound_score >= 0.05:
            sentiment_class = "Positive"
        elif (compound_score > -0.05) and (compound_score < 0.05):
            sentiment_class = "Neutral"
        else:
            sentiment_class = "Negative"
        print(tweet+"\t The sentiment of tweet is : " + sentiment_class)
        # add text and sentiment info to elasticsearch
        elastic.index(index="biden_tweets",
                                 doc_type="test-type",
                                 body={"author": msg_data["user"]["screen_name"],
                                       "date": msg_data["created_at"],
                                       "message": msg_data["text"],
                                       "positive_score": positive_score,
                                       "negative_score": negative_score,
                                       "neutral_score": neutral_score,
                                       "compound_score": compound_score,
                                       "sentiment_class": sentiment_class})
        print('\n')


if __name__ == "__main__":
    main()
