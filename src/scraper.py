import time
from contextlib import suppress

from pymongo.errors import DuplicateKeyError
from steem.utils import is_comment
from steemdata.blockchain import Blockchain, typify

from helpers import fetch_price_feed, get_usernames_batch, extract_usernames_from_op
from methods import update_account, update_account_ops, upsert_post
from mongostorage import MongoStorage, Settings, Stats
from tasks import update_account_async, update_post_async


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
        update_account_ops(mongo, steem, username)
        s.set_account_checkpoint(username)
        print('Updated @%s' % username)

    # this was the last batch
    if account_checkpoint and len(usernames) < 1000:
        s.set_account_checkpoint(-1)


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
        # if operation is a main Post, update it in background
        if operation['type'] == 'comment':
            if not operation['parent_author'] and not is_comment(operation):
                post_identifier = "@%s/%s" % (operation['author'], operation['permlink'])
                update_post_async.delay(post_identifier)

        # if we are up to date, trigger an update for referenced accounts
        if last_block > blockchain.get_current_block_num() - 100:
            for acc in extract_usernames_from_op(operation):
                update_account_async.delay(acc)

        # insert operation
        with suppress(DuplicateKeyError):
            mongo.Operations.insert_one(typify(operation))

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
    """ Fetch prices and stuff...
    """
    while True:
        Stats(mongo).refresh()
        prices = fetch_price_feed()
        mongo.PriceHistory.insert_one(prices)
        time.sleep(60 * 60)


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
