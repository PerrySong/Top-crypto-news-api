# -*- coding: utf-8 -*-

import os
import sys

# import common package in parent directory
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'scrapers'))

import cnn_news_scraper
from cloudAMQP_client import CloudAMQPClient 

DEDUPE_NEWS_TASK_QUEUE_URL = "amqp://hstwbclj:wJyANU7kAFtbtCFqFeovylGxaHgX7ArM@otter.rmq.cloudamqp.com/hstwbclj"
DEDUPE_NEWS_TASK_QUEUE_NAME = "tap-news-dedupe-news-task-queue"
SCRAPE_NEWS_TASK_QUEUE_URL = "amqp://jsezaicq:wosEhOYiT42GTfWmquL4IIyBs0N0f-O4@otter.rmq.cloudamqp.com/jsezaicq"
SCRAPE_NEWS_TASK_QUEUE_NAME = "tap-news-scrape-news-task-queue"

SLEEP_TIME_IN_SECONDS = 5

dedupe_news_queue_client = CloudAMQPClient(DEDUPE_NEWS_TASK_QUEUE_URL, DEDUPE_NEWS_TASK_QUEUE_NAME)
scrape_news_queue_client = CloudAMQPClient(SCRAPE_NEWS_TASK_QUEUE_URL, SCRAPE_NEWS_TASK_QUEUE_NAME)

def handle_message(msg):
    if msg is None or not isinstance(msg, dict): #isinstance == java instanceOf()
        print('message is broken')
        return 
    
    task = msg
    text = None

    # Now we support CNN only 
    if task['source'] == 'cnn':
        print('Scraping CNN news: ')
        text = cnn_news_scraper.extract_news(task['url'])
        print(text)
    else:
        print('News source [%s] is not supported.' % task['source'])

    task['text'] = text
    # deduper_news_queue_client.sendMessage(task)


while True:
    # fetch message from queue
    if scrape_news_queue_client is not None:
        msg = scrape_news_queue_client.getMessage()
        if msg is not None:
        # Handle message
            try: 
                handle_message(msg)
            except Exception as e:
                print('hey')
                print(e)
                pass # == continue
        
        scrape_news_queue_client.sleep(SLEEP_TIME_IN_SECONDS)
