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


def update_account(mongo, steem, username, min_age=60):
    """ Update Account. If minimum refresh age is not reached, skip the update."""
    if min_age:
        acc = mongo.Accounts.find_one({'name': username}, {'_id': 0, 'updatedAt': 1})
        if acc and acc['updatedAt'] > (dt.datetime.utcnow() - dt.timedelta(seconds=min_age)):
            return

    a = Account(username, steem_instance=steem)
    account = {
        **typify(a.export()),
        'account': username,
        'updatedAt': dt.datetime.utcnow(),
    }
    mongo.Accounts.update({'name': a.name}, account, upsert=True)


def update_account_ops(mongo, steem, username):
    """ This method will fetch entire account history, and back-fill any missing ops. """
    for event in Account(username, steem_instance=steem).history():
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


def update_account_ops_quick(mongo, steem, username):
    start_index = account_operations_index(mongo, username)

    # fetch latest records and update the db
    for event in quick_history(steem, username, start_index):
        with suppress(DuplicateKeyError):
            mongo.AccountOperations.insert_one(typify(event))


def quick_history(steem, username, last_known_index=0):
    """ This method will fetch last 1000 account operations in a single RPC call.
    Unnecessary results will be filtered out, if last_known_index is specified."""
    history = steem.rpc.get_account_history(username, -1, 1000)

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
