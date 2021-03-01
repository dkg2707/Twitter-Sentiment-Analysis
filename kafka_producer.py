import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from kafka -import KafkaProducer
import kafka
import json

# TWITTER API CONFIGURATIONS
consumer_key = "wYBXebq8lMxcPR30v8jDuAiw9"
consumer_secret = "pHPnl5rU5uuEBvcI4b3yPTypuZtK2sjAQD0QdXZfZ8P4lOtGIC"
access_token = "173364076-BBl52vTVMwe5L9onYLoQqxvku84VbquTH5V0vy64"
access_secret = "YejrGJJio2ZKGSEYiahcGxMsi4dTlkQbE2CAdtZ9NIsbe"

# TWITTER API AUTH
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)


# Twitter Stream Listener
class KafkaPushListener(StreamListener):
    def __init__(self):
        # localhost:9092 = Default Zookeeper Producer Host and Port Adresses
        self.kafka_producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    # Get Producer that has topic name is Twitter
    # self.kafka_producer = self.client.topics[bytes("twitter")].get_producer()

    def on_data(self, data):
        # Producer produces data for consumer
        # Data comes from Twitter
        self.kafka_producer.send("twitter_sentiment", data.encode('utf-8'))
        print(data)
        return True

    def on_error(self, status):
        print(status)
        return True


# Twitter Stream Config
twitter_stream = Stream(auth, KafkaPushListener())

# Produce Data that has trump hashtag (Tweets)
twitter_stream.filter(track=['#joebiden'])
