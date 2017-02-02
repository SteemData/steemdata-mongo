import datetime as dt
from contextlib import suppress

import pymongo
from pymongo.errors import DuplicateKeyError
from steem.account import Account
from steem.exceptions import PostDoesNotExist
from steem.post import Post
from steemdata.blockchain import typify


def upsert_post(mongo, post_identifier, steem=None):
    with suppress(PostDoesNotExist):
        p = Post(post_identifier, steem_instance=steem)

        # scrape post and its replies
        entry = {
            **p.export(),
            'replies': [],
            # 'replies': [x.export() for x in _fetch_comments_flat(p)],
        }
        return mongo.Posts.update({'identifier': p.identifier}, entry, upsert=True)


def update_account(mongo, steem, username):
    a = Account(username, steem_instance=steem)
    account = {
        **typify(a.export()),
        "updatedAt": dt.datetime.utcnow(),
    }
    mongo.Accounts.update({'name': a.name}, account, upsert=True)


def update_account_ops(mongo, steem, username, from_last_index=True):
    # check the highest index in the database
    start_index = 0
    if from_last_index:
        highest_index = list(mongo.AccountOperations.find({'account': username}).
                             sort("index", pymongo.DESCENDING).limit(1))
        if highest_index:
            start_index = highest_index[0].get('index', 0)

    # fetch missing records and update the db
    for event in Account(username, steem_instance=steem).history(start=start_index):
        with suppress(DuplicateKeyError):
            mongo.AccountOperations.insert_one(typify(event))
