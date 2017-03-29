import datetime as dt
from contextlib import suppress

import pymongo
from pymongo.errors import DuplicateKeyError
from steem import Steem
from steem.account import Account
from steem.post import Post
from steembase.exceptions import PostDoesNotExist
from steemdata.blockchain import typify


def upsert_post(mongo, post_identifier):
    with suppress(PostDoesNotExist):
        p = Post(post_identifier)

        # scrape post and its replies
        entry = {
            **p.export(),
            'replies': [],
            # 'replies': [x.export() for x in _fetch_comments_flat(p)],
        }
        return mongo.Posts.update({'identifier': p.identifier}, entry, upsert=True)


def update_account(mongo, username, load_extras=True):
    """ Update Account. If load_extras """
    a = Account(username)
    account = {
        **typify(a.export(load_extras=load_extras)),
        'account': username,
        'updatedAt': dt.datetime.utcnow(),
    }
    if not load_extras:
        account = {'$set': account}
    mongo.Accounts.update({'name': a.name}, account, upsert=True)


def update_account_ops(mongo, username):
    """ This method will fetch entire account history, and back-fill any missing ops. """
    for event in Account(username).history():
        with suppress(DuplicateKeyError):
            mongo.AccountOperations.insert_one(typify(event))


def account_operations_index(mongo, username):
    """ Lookup AccountOperations for latest synced index. """
    start_index = 0
    # use projection to ensure covered query
    highest_index = list(mongo.AccountOperations.find({'account': username}, {'_id': 0, 'index': 1}).
                         sort("index", pymongo.DESCENDING).limit(1))
    if highest_index:
        start_index = highest_index[0].get('index', 0)

    return start_index


def update_account_ops_quick(mongo, username):
    start_index = account_operations_index(mongo, username)

    # fetch latest records and update the db
    for event in quick_history(username, start_index):
        with suppress(DuplicateKeyError):
            mongo.AccountOperations.insert_one(typify(event))


def quick_history(username, last_known_index=0):
    """ This method will fetch last 1000 account operations in a single RPC call.
    Unnecessary results will be filtered out, if last_known_index is specified."""
    s = Steem()
    history = s.get_account_history(username, -1, 1000)

    results = []
    for item in history:
        if last_known_index >= item[0]:
            continue

        index, block = item
        op_type, op = block['op']
        timestamp = block['timestamp']
        trx_id = block['trx_id']

        results.append({
            **op,
            'index': index,
            'account': username,
            'trx_id': trx_id,
            'timestamp': timestamp,
            'type': op_type,
        })

    return results
