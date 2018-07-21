# -*- coding: utf-8 -*-

import datetime
import hashlib
import os
import sys
import redis

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))

import news_api_client
from cloudAMQP_client import CloudAMQPClient
SLEEP_TIME_IN_SECONDS = 60 * 2

REDIS_HOST = 'localhost'
REDIS_PORT = 6379

NEWS_TIME_OUT_IN_SECONDS = 3600 * 24

SCRAPE_NEWS_TASK_QUEUE_URL = "amqp://ytprcnml:zGEBaewr54zh8rmvL1JYhqeA4QdVTXyN@otter.rmq.cloudamqp.com/ytprcnml"
SCRAPE_NEWS_TASK_QUEUE_NAME = "test"

# SCRAPE_NEWS_TASK_QUEUE_URL = "amqp://jsezaicq:wosEhOYiT42GTfWmquL4IIyBs0N0f-O4@otter.rmq.cloudamqp.com/jsezaicq"
# SCRAPE_NEWS_TASK_QUEUE_NAME = "tap-news-scrape-news-task-queue"

NEWS_SOURCES = [
    'cnn'
]

def run():
    redis_client = redis.StrictRedis(REDIS_HOST, REDIS_PORT)
    scrape_news_cloudAMQP_client = CloudAMQPClient(SCRAPE_NEWS_TASK_QUEUE_URL, SCRAPE_NEWS_TASK_QUEUE_NAME)

    while True:

        news_list = news_api_client.getNewsFromSource(NEWS_SOURCES)

        num_of_new_news = 0

        for news in news_list:
            news_digest = hashlib.md5(news['title'].encode('utf-8')).hexdigest()

            # Check if the news have already been added to the queue
            print('hahahaha')
            print(f'digest = {news_digest}')
            print(redis_client.get(news_digest))

            # If news is already in redis, continue
            if redis_client.get(news_digest):
                continue

            #Add publishedAt if a news do not has that
            if not news['publishedAt']:
                # YYYY-MM-DDTHH:MM:SS in UTC
                news['publishedAt'] = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            
            num_of_new_news = num_of_new_news + 1
            # Add digest to news
            news['digest'] = news_digest
            # Add news to redis
            redis_client.set(news_digest, news)
            redis_client.expire(news_digest, NEWS_TIME_OUT_IN_SECONDS)
            # Send news to queue
            print(f'Sending news: {news}')
            scrape_news_cloudAMQP_client.sendMessage(news)

        print("Fetched %d new news." % num_of_new_news)

        # 
        scrape_news_cloudAMQP_client.sleep(SLEEP_TIME_IN_SECONDS)

if __name__ == '__main__':
    run()