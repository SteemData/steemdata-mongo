import logging
import time
from contextlib import suppress

from funcy import compose
from pymongo.errors import DuplicateKeyError
from steem import Steem
from steem.blockchain import Blockchain
from steemdata.utils import (
    json_expand,
    typify,
)
from toolz import partition_all

from methods import (
    update_account,
    update_account_ops,
)
from mongostorage import Indexer, Stats
from utils import (
    fetch_price_feed,
    get_usernames_batch,
    strip_dot_from_keys,
)

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


# Operations
# ----------
def scrape_operations(mongo):
    """Fetch all operations (including virtual) from last known block forward."""
    indexer = Indexer(mongo)
    blockchain = Blockchain(mode="irreversible")
    last_block = indexer.get_checkpoint('operations')

    history = blockchain.history(
        start_block=last_block,
    )

    log.info('\n> Fetching operations, starting with block %d...' % last_block)
    for operation in history:
        # insert operation
        with suppress(DuplicateKeyError):
            transform = compose(strip_dot_from_keys, json_expand, typify)
            mongo.Operations.insert_one(transform(operation))

        # if this is a new block, checkpoint it, and schedule batch processing
        if operation['block_num'] != last_block:
            last_block = operation['block_num']
            indexer.set_checkpoint('operations', last_block - 1)

            if last_block % 10 == 0:
                log.info("Checkpoint #%s: (%s)" % (
                    last_block,
                    blockchain.steem.hostname
                ))


# Accounts, AccountOperations
# ---------------------------
def scrape_all_users(mongo, quick=False):
    """
    Scrape all existing users
    and insert/update their entries in Accounts collection.
    """
    steem = Steem()
    indexer = Indexer(mongo)

    account_checkpoint = indexer.get_checkpoint('accounts')
    if account_checkpoint:
        usernames = list(get_usernames_batch(account_checkpoint, steem))
    else:
        usernames = list(get_usernames_batch(steem))

    for username in usernames:
        update_account(mongo, username, load_extras=quick)
        if not quick:
            update_account_ops(mongo, username)
        indexer.set_checkpoint('accounts', username)
        log.info('Updated @%s' % username)

    # this was the last batch
    if account_checkpoint and len(usernames) < 1000:
        indexer.set_checkpoint('accounts', -1)


# Blockchain
# ----------
def scrape_blockchain(mongo):
    s = Steem()
    # see how far behind we are
    missing = list(range(last_block_num(mongo), s.last_irreversible_block_num))

    # if we are far behind blockchain head
    # split work in chunks of 100
    if len(missing) > 100:
        for batch in partition_all(100, missing):
            results = s.get_blocks(batch)
            insert_blocks(mongo, results)

    # otherwise continue as normal
    blockchain = Blockchain(mode="irreversible")
    hist = blockchain.stream_from(start_block=last_block_num(mongo), full_blocks=True)
    insert_blocks(mongo, hist)


def insert_blocks(mongo, full_blocks):
    for block in full_blocks:
        if not block.get('block_num'):
            block['block_num'] = int(block['block_id'][:8], base=16)

        if block['block_num'] > 1:
            assert block_id_exists(mongo, block['previous']), \
                'Missing Previous Block (%s)' % block['previous']

        with suppress(DuplicateKeyError):
            mongo.db['Blockchain'].insert_one(block)


def block_id_exists(mongo, block_id: str):
    # covered query
    return mongo.db['Blockchain'].find_one(
        {'block_id': block_id}, {'_id': 0, 'block_id': 1})


def last_block_num(mongo) -> int:
    return mongo.db['Blockchain'].find_one(
        filter={},
        projection={'_id': 0, 'block_num': 1},
        sort=[('block_num', -1)]
    ).get('block_id', 1)


# Misc
# ----
def refresh_dbstats(mongo):
    while True:
        Stats(mongo).refresh()
        time.sleep(60)


def scrape_prices(mongo):
    """ Update PriceHistory every hour.
    """
    while True:
        prices = fetch_price_feed()
        mongo.PriceHistory.insert_one(prices)
        time.sleep(60 * 5)


def run():
    from mongostorage import MongoStorage
    from steemdata.helpers import timeit
    m = MongoStorage()
    m.ensure_indexes()
    with timeit():
        scrape_operations(m)
        # update_account(m, 'furion', load_extras=True)
        # update_account_ops(m, 'furion')
        # scrape_all_users(m, False)
        # validate_operations(m)


if __name__ == '__main__':
    with suppress(KeyboardInterrupt):
        run()
