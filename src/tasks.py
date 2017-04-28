import os

from celery import Celery

from methods import update_account, update_account_ops_quick, upsert_comment_chain
from mongostorage import MongoStorage, DB_NAME, MONGO_HOST, MONGO_PORT
from utils import log_exceptions

mongo = MongoStorage(db_name=os.getenv('DB_NAME', DB_NAME),
                     host=os.getenv('DB_HOST', MONGO_HOST),
                     port=os.getenv('DB_PORT', MONGO_PORT))
mongo.ensure_indexes()


def new_celery(worker_name: str):
    return Celery(worker_name,
                  backend=os.getenv('CELERY_BACKEND_URL', 'redis://localhost:6379/0'),
                  broker=os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/0'))


tasks = new_celery('tasks')


@tasks.task
def update_account_async(account_name, load_extras=False):
    update_account(mongo, account_name, load_extras=load_extras)
    update_account_ops_quick(mongo, account_name)


@tasks.task
def update_comment_async(post_identifier, recursive=False):
    upsert_comment_chain(mongo, post_identifier, recursive)


@tasks.task
def batch_update_async(batch_items: dict):
    # todo, we can make acc updates faster by injecting Converter instance
    for account_name in batch_items['accounts']:
        with log_exceptions():
            update_account(mongo, account_name, load_extras=True)
            update_account_ops_quick(mongo, account_name)
    for account_name in batch_items['accounts_light']:
        with log_exceptions():
            update_account(mongo, account_name, load_extras=False)
            update_account_ops_quick(mongo, account_name)
    for identifier in batch_items['comments']:
        with log_exceptions():
            upsert_comment_chain(mongo, identifier, recursive=True)
