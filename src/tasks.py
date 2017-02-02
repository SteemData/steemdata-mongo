from celery import Celery

from scraper import update_account, upsert_post

app = Celery('tasks', backend='rpc://', broker='amqp://localhost')


@app.task
def add(x, y):
    return x + y


@app.task
def update_account_async(account_name):
    update_account(mongo, steem, account_name)


@app.task
def update_post_async(post_identifier):
    upsert_post(mongo, post_identifier)
