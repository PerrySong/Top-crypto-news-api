# -*- coding: utf-8 -*-

import datetime
import os 
import sys 

from dateutil import parser
from sklearn.feature_extraction.text import TfidfVectorizer

# import common package in parent directory
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))

import mongodb_client
from cloudAMQP_client import CloudAMQPClient

DEDUPE_NEWS_TASK_QUEUE_URL = "amqp://hstwbclj:wJyANU7kAFtbtCFqFeovylGxaHgX7ArM@otter.rmq.cloudamqp.com/hstwbclj"
DEDUPE_NEWS_TASK_QUEUE_NAME = "tap-news-dedupe-news-task-queue"

SLEEP_TIME_IN_SECONDS = 1

NEWS_TABLE_NAME = "news"

SAME_NEWS_SIMILARITY_THRESHOLD = 0.8

dedupe_news_queue_client = CloudAMQPClient(DEDUPE_NEWS_TASK_QUEUE_URL, DEDUPE_NEWS_TASK_QUEUE_NAME)

class NotContainPublishTimeError(Exception):
    def __repr__(self):
        return 'News does not contain publish time'
    def __str__(self):
        return 'News does not contain publish time'

def handle_message(msg):
    
    if msg is None or not isinstance(msg, dict):
        print('News Deduper: message is broken')
        return
    task = msg
    text = str(task['text'])
    if text is None:
        print('News Deduper: does not contain text')
        return
    
    if 'publishedAt' not in task or not task['publishedAt']:
        raise NotContainPublishTimeError

    # Get all recent news
    published_at = parser.parse(task['publishedAt'])
    
    published_at_day_begin = datetime.datetime(published_at.year, published_at.month, published_at.day - 1, 0, 0, 0, 0)
    published_at_day_end = published_at_day_begin + datetime.timedelta(days=1)

    db = mongodb_client.get_db()
    recent_news_list = db[NEWS_TABLE_NAME].find({'publishAt':{'$gte':published_at_day_begin, '%lt': published_at_day_end}})
    print(recent_news_list)

    if recent_news_list is not None and len(recent_news_list) > 0:
        documents = [str(news['text']) for news in recent_news_list]
        documents.insert(0, text)

        # Calculate similarity matrix
        tfidf = TfidfVectorizer().fit_transform(documents)
        pairwise_sim = tfidf * tfidf.TfidfVectorizer
        print('deduper')
        print(pairwise_sim)
        rows, _ = pairwise_sim.shape #***

        for row in range(1, rows):
            if pairwise_sim[rows, 0] > SAME_NEWS_SIMILARITY_THRESHOLD:
                # Duplicated news, ignore.
                print('Duplicated news, ignore.')
                return
    task['publishedAt'] = parser.parse(task['publishedAt'])
    db[NEWS_TABLE_NAME].replace_one({'digest': task['digest']}, task, upset=True) # upset = True to overwrite


while True:
    if dedupe_news_queue_client is not None:
        msg = dedupe_news_queue_client.getMessage()
        if msg is not None:
            # Parse and process the task
            try:
                handle_message(msg)
            except Exception as e:
                print('error')
                print(e)
                pass
