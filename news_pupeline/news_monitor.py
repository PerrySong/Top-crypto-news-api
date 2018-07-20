# -*- coding: utf-8 -*-

import datetime
import hashlib
import os
import sys
import redis

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))

import news_api_client
from cloudAMQP_client import CloudAMQPClient
SLEEP_TIME_IN_SECONDS = 10

REDIS_HOST = 'localhost'
REDIS_PORT = 6379

NEWS_TIME_OUT_IN_SECONDS = 3600 * 24

SCRAPE_NEWS_TASK_QUEUE_URL = "amqp://jsezaicq:wosEhOYiT42GTfWmquL4IIyBs0N0f-O4@otter.rmq.cloudamqp.com/jsezaicq"
SCRAPE_NEWS_TASK_QUEUE_NAME = "tap-news-scrape-news-task-queue"

NEWS_SOURCES = [
    'cnn'
]

def run():
    redis_client = redis.StrictRedis(REDIS_HOST, REDIS_PORT)
    cloudAMQP_client = CloudAMQPClient(SCRAPE_NEWS_TASK_QUEUE_URL, SCRAPE_NEWS_TASK_QUEUE_NAME)

    while True:
        news_list = news_api_client.getNewsFromSource(NEWS_SOURCES)

        num_of_new_news = 0

        for news in news_list:
            news_digest = hashlib.md5(news['title'].encode('utf-8')).hexdigest()

            # Check if the news have already been added to the queue
            if redis_client.get(news_digest) is None:
                num_of_new_news = num_of_new_news + 1
                news['digest'] = news_digest

                if news['publishedAt'] is None:
                    # YYYY-MM-DDTHH:MM:SS in UTC
                    news['publish'] = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

            redis_client.set(news_digest, news)
            redis_client.expire(news_digest, NEWS_TIME_OUT_IN_SECONDS)

            print(news)

            cloudAMQP_client.sendMessage(news)

        print("Fetched %d new news." % num_of_new_news)

        cloudAMQP_client.sleep(SLEEP_TIME_IN_SECONDS)

if __name__ == '__main__':
    run()