# -*- coding: utf-8 -*-

import requests
import os
import random

from lxml import html

#Need to find the pattern, using chrome tool: "xpath helper", click it and press 'shift'
GET_CNN_NEWS_XPATH = '''//p[@class='zn-body__paragraph speakable']//text() | //div[@class='zn-body__paragraph speakable']//text() | //div[@class='zn-body__paragraph speakable']//text()'''

USER_AGENTS_FILE = os.path.join(os.path.dirname(__file__), 'user_agents.txt')
USER_AGENTS = []

with open(USER_AGENTS_FILE, 'r') as uaf: 
    print(uaf.readline())
    for ua in uaf.readlines():
        if ua:
            # print(ua)
            USER_AGENTS.append(ua.strip()[1:-1])
random.shuffle(USER_AGENTS)

def getHeaders():
    ua = random.choice(USER_AGENTS)
    headers = {
        "Connection" : "close",
        "User-Agent" : ua
    }
    return headers


def extract_news(news_url):
    # 1. Fetch html
    # Web site might use session to judge if you are a bot
    # User agent
    session_requests = requests.session()
    response = session_requests.get(news_url, headers=getHeaders())

    # 2. Parse html
    try:
        tree = html.fromstring(response.content)
        news = tree.xpath(GET_CNN_NEWS_XPATH)
        news = ''.join(news)
    except Exception as e:
        print(e)
        return {}
    # 3. Extract information

    return news