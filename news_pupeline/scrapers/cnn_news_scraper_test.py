import cnn_news_scraper as scraper 

EXPECTED_STRING = ''
CNN_NEWS_URL = "https://www.cnn.com/2018/07/20/politics/donald-trump-robert-mueller-helsinki-summit/index.html"

def test_basic():
    news = scraper.extract_news(CNN_NEWS_URL)

    print(news)

if __name__ == "__main__":
    test_basic()