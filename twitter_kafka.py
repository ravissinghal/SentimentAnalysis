import tweepy
import time
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
import re

consumer_key = ""
consumer_secret_key = ""
access_token = ""
access_token_secret = ""

auth = tweepy.OAuthHandler(consumer_key, consumer_secret_key)
auth.set_access_token(access_token,access_token_secret)
api = tweepy.API(auth)

producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic_name = 'first_topic'

def get_twitter_data():
    res = api.search_tweets("Elon Musk")
    print(res)
    for i in res:
        record = ''
        record += str(i.text)
        raw_tweet = ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)"," ",record).split())
        producer.send(topic_name, str.encode(raw_tweet))
        
get_twitter_data()

def periodic_work(interval):
    while True:
        get_twitter_data()
        time.sleep(interval)

periodic_work(60*0.1)
