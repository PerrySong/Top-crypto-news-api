from pymongo import MongoClient
# connection thread pool

MONGO_DB_HOST = 'localhost'
MONGO_DB_PORT = 27017
DB_NAME = 'tap-news'

client = MongoClient(MONGO_DB_HOST, MONGO_DB_PORT)

# db=DB_NAME ? if db == null, db = DB_NAME
def get_db(db=DB_NAME):
    db = client[db]
    return db
