import os

from celery import Celery
from steem import Steem

from methods import update_account, upsert_post, update_account_ops_quick
from mongostorage import MongoStorage, DB_NAME, MONGO_HOST, MONGO_PORT

app = Celery('tasks',
             backend=os.getenv('CELERY_BACKEND_URL', 'redis://localhost:6379/0'),
             broker=os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/0'))

mongo = MongoStorage(db_name=os.getenv('DB_NAME', DB_NAME),
                     host=os.getenv('DB_HOST', MONGO_HOST),
                     port=os.getenv('DB_PORT', MONGO_PORT))
mongo.ensure_indexes()
stm = Steem()


@app.task
def update_account_async(account_name):
    update_account(mongo, stm, account_name)
    update_account_ops_quick(mongo, stm, account_name)


@app.task
def update_post_async(post_identifier):
    upsert_post(mongo, post_identifier)
