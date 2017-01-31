import time
from contextlib import suppress

import pymongo
from funcy import flatten
from pymongo.errors import DuplicateKeyError
from steem import Steem
from steem.account import Account
from steem.exceptions import PostDoesNotExist
from steem.post import Post
from steem.utils import is_comment, parse_time
from steemdata.blockchain import Blockchain, typify

from helpers import fetch_price_feed
from mongostorage import MongoStorage, Settings, Stats


def scrape_all_users(mongo, steem=None):
    """Scrape all existing users and insert/update their entries in Accounts collection."""
    s = Settings(mongo)

    account_checkpoint = s.account_checkpoint()
    if account_checkpoint:
        usernames = list(get_usernames_batch(account_checkpoint, steem))
    else:
        usernames = list(get_usernames_batch(steem))

    for username in usernames:
        update_account(mongo, steem, username)
        s.set_account_checkpoint(username)
        print('Scraped account data for %s' % username)

    if account_checkpoint and len(usernames) < 1000:
        s.set_account_checkpoint(-1)


def scrape_virtual_operations(mongo, steem=None):
    """ Fetch all virtual operations for all users.
    """
    s = Settings(mongo)

    virtual_op_checkpoint = s.virtual_op_checkpoint()
    if virtual_op_checkpoint:
        usernames = list(get_usernames_batch(virtual_op_checkpoint, steem))
    else:
        usernames = list(get_usernames_batch(steem))

    for username in usernames:
        update_account_ops(mongo, steem, username)
        s.set_virtual_op_checkpoint(username)
        print('Scraped virtual ops for %s' % username)

    if virtual_op_checkpoint and len(usernames) < 1000:
        s.set_virtual_op_checkpoint(-1)


def scrape_operations(mongo, steem=None):
    """Fetch all operations from last known block forward."""
    settings = Settings(mongo)
    blockchain = Blockchain(mode="irreversible", steem_instance=steem)
    last_block = settings.last_block()

    history = blockchain.replay(
        start_block=last_block,
    )
    print('\n> Fetching operations, starting with block %d...' % last_block)
    for operation in history:
        # if operation is a main Post, update Posts collection
        if operation['type'] == 'comment':
            if not operation['parent_author'] and not is_comment(operation):
                post_identifier = "@%s/%s" % (operation['author'], operation['permlink'])
                upsert_post(mongo, post_identifier)

        # if operation is a new account, add it to Accounts
        if operation['type'] == 'account_create':
            update_account(mongo, steem, operation['new_account_name'])

        # parse fields
        operation = typify(operation)

        # insert operation
        with suppress(DuplicateKeyError):
            mongo.Operations.insert_one(operation)

        # current block - 1 should become a new checkpoint
        if operation['block_num'] != last_block:
            last_block = operation['block_num']
            settings.update_last_block(last_block - 1)

        print("%s: #%s" % (operation['timestamp'], operation['block_num']))


def scrape_active_posts(mongo, steem=None):
    """ Update all non-archived posts.
    """
    posts_cursor = mongo.Posts.find({'mode': {'$ne': 'archived'}}, no_cursor_timeout=True)
    posts_count = posts_cursor.count()
    print('Updating %s active posts...' % posts_count)
    for post in posts_cursor:
        upsert_post(mongo, post['identifier'], steem=steem)
    posts_cursor.close()


def scrape_misc(mongo):
    """Fetch prices and stuff...
    """
    while True:
        Stats(mongo).refresh()
        prices = fetch_price_feed()
        mongo.PriceHistory.insert_one(prices)
        time.sleep(60 * 60)


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
    mongo.Accounts.update({'name': a.name}, a.export(), upsert=True)


def update_account_ops(mongo, steem, username):
    # check the highest index in the database
    start_index = 0
    highest_index = list(mongo.AccountOperations.find({'account': username}).
                         sort("index", pymongo.DESCENDING).limit(1))
    if highest_index:
        start_index = highest_index[0].get('index', 0)

    # fetch missing records and update the db
    for event in Account(username, steem_instance=steem).history(start=start_index):
        with suppress(DuplicateKeyError):
            # parse fields
            event = {**event, 'timestamp': parse_time(event['timestamp'])}
            mongo.AccountOperations.insert_one(event)


def _fetch_comments_flat(root_post=None, comments=list(), all_comments=list()):
    """
    Recursively fetch all the child comments, and return them as a list.

    Usage: all_comments = fetch_comments_flat(Post('@foo/bar'))
    """
    # see if our root post has any comments
    if root_post:
        return _fetch_comments_flat(comments=root_post.get_comments())
    if not comments:
        return all_comments

    # recursively scrape children one depth layer at a time
    children = list(flatten([x.get_comments() for x in comments]))
    if not children:
        return all_comments
    return _fetch_comments_flat(all_comments=comments + children, comments=children)


def get_all_usernames(last_user=-1, steem=None):
    if not steem:
        steem = Steem()

    usernames = steem.rpc.lookup_accounts(last_user, 1000)
    batch = []
    while len(batch) != 1:
        batch = steem.rpc.lookup_accounts(usernames[-1], 1000)
        usernames += batch[1:]

    return usernames


def get_usernames_batch(last_user=-1, steem=None):
    if not steem:
        steem = Steem()

    return steem.rpc.lookup_accounts(last_user, 1000)


def override(mongo):
    """Various fixes to avoid re-scraping"""
    return
    # for op in mongo.Operations.find({"timestamp": {'$type': "string"}}, no_cursor_timeout=True).limit(100000):
    #     print("O: %s" % op['_id'])
    #     if type(op['timestamp']) == str:
    #         mongo.Operations.update_one({"_id": op['_id']}, {"$set": {"timestamp": parse_time(op['timestamp'])}})


def test():
    m = MongoStorage()
    m.ensure_indexes()
    # scrape_misc(m)
    # scrape_all_users(m, Steem())
    scrape_operations(m)
    # scrape_virtual_operations(m)
    # scrape_active_posts(m)


if __name__ == '__main__':
    test()
