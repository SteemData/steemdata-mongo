import datetime as dt
from contextlib import suppress

import pymongo
from funcy.seqs import take
from pymongo.errors import DuplicateKeyError
from steem.account import Account
from steem.post import Post
from steembase.exceptions import PostDoesNotExist
from steemdata.blockchain import typify


def upsert_post(mongo, post_identifier):
    with suppress(PostDoesNotExist):
        p = Post(post_identifier)
        return mongo.Posts.update({'identifier': p.identifier}, p.export(), upsert=True)


def upsert_comment(mongo, post_identifier):
    with suppress(PostDoesNotExist):
        p = Post(post_identifier)
        return mongo.Comments.update({'identifier': p.identifier}, p.export(), upsert=True)


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


def update_account_ops_quick(mongo, username, batch_size=200, steemd_instance=None):
    """ Only update the latest missing history, limited to 1 batch of defined batch_size. """
    start_index = account_operations_index(mongo, username)

    # fetch latest records and update the db
    history = Account(username, steemd_instance=steemd_instance).history_reverse(batch_size=batch_size)
    for event in take(batch_size, history):
        if event['index'] < start_index:
            return
        with suppress(DuplicateKeyError):
            mongo.AccountOperations.insert_one(typify(event))
