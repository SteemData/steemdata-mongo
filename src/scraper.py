import datetime as dt
import logging
import time
from contextlib import suppress

from funcy import (
    compose,
    lpluck,
    lkeep,
    flatten,
    merge_with,
    keep,
    lfilter,
    silent,
)
from pymongo import UpdateOne
from pymongo.errors import DuplicateKeyError
from steem import Steem
from steem.blockchain import Blockchain
from steem.post import Post
from steembase.exceptions import PostDoesNotExist
from steemdata.utils import (
    json_expand,
    typify,
)
from toolz import partition_all

from methods import (
    update_account,
    update_account_ops,
    update_account_ops_quick,
    upsert_comment_chain,
    parse_operation,
)
from mongostorage import Indexer, Stats
from utils import (
    fetch_price_feed,
    get_usernames_batch,
    strip_dot_from_keys,
    thread_multi,
    log_exceptions,
)

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


# Operations
# ----------
def scrape_operations(mongo):
    """Fetch all operations (including virtual) from last known block forward."""
    indexer = Indexer(mongo)
    last_block = indexer.get_checkpoint('operations')
    log.info('\n> Fetching operations, starting with block %d...' % last_block)

    blockchain = Blockchain(mode="irreversible")
    history = blockchain.history(
        start_block=last_block,
    )
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
                log.info("Checkpoint: %s (%s)" % (
                    last_block,
                    blockchain.steem.hostname
                ))


# Posts, Comments
# ---------------
def scrape_comments(mongo, batch_size=250, max_workers=50):
    """ Parse operations and post-process for comment/post extraction. """
    indexer = Indexer(mongo)
    start_block = indexer.get_checkpoint('comments')

    query = {
        "type": "comment",
        "block_num": {
            "$gt": start_block,
            "$lte": start_block + batch_size,
        }
    }
    projection = {
        '_id': 0,
        'block_num': 1,
        'author': 1,
        'permlink': 1,
    }
    results = list(mongo.Operations.find(query, projection=projection))
    identifiers = set(f"{x['author']}/{x['permlink']}" for x in results)

    # handle an edge case when we are too close to the head,
    # and the batch contains no work to do
    if not results and is_recent(start_block, days=1):
        return

    def get_post(identifier):
        with suppress(PostDoesNotExist):
            return strip_dot_from_keys(Post(identifier).export())

    # get Post.export() results in parallel
    raw_comments = thread_multi(
        fn=get_post,
        fn_args=[None],
        dep_args=list(identifiers),
        max_workers=max_workers,
        yield_results=True
    )
    raw_comments = lkeep(raw_comments)

    # split into root posts and comments
    posts = lfilter(lambda x: x['depth'] == 0, raw_comments)
    comments = lfilter(lambda x: x['depth'] > 0, raw_comments)

    # Mongo upsert many
    log_output = ''
    if posts:
        r = mongo.Posts.bulk_write(
            [UpdateOne({'identifier': x['identifier']},
                       {'$set': {**x, 'updatedAt': dt.datetime.utcnow()}},
                       upsert=True)
             for x in posts],
            ordered=False,
        )
        log_output += \
            f'(Posts: {r.upserted_count} upserted, {r.modified_count} modified) '
    if comments:
        r = mongo.Comments.bulk_write(
            [UpdateOne({'identifier': x['identifier']},
                       {'$set': {**x, 'updatedAt': dt.datetime.utcnow()}},
                       upsert=True)
             for x in comments],
            ordered=False,
        )
        log_output += \
            f'(Comments: {r.upserted_count} upserted, {r.modified_count} modified) '

    # We are only querying {type: 'comment'} blocks and sometimes
    # the gaps are larger than the batch_size.
    index = silent(max)(lpluck('block_num', results)) or (start_block + batch_size)
    indexer.set_checkpoint('comments', index)

    log.info(f'Checkpoint: {index} {log_output}')


# Accounts, AccountOperations
# ---------------------------
def scrape_all_users(mongo, quick=False):
    """
    Scrape all existing users
    and insert/update their entries in Accounts collection.

    Ideally, this would only need to run once, because "scrape_accounts"
    takes care of accounts that need to be updated in each block.
    """
    steem = Steem()
    indexer = Indexer(mongo)

    account_checkpoint = indexer.get_checkpoint('accounts')
    if account_checkpoint:
        usernames = list(get_usernames_batch(account_checkpoint, steem))
    else:
        usernames = list(get_usernames_batch(steem))

    for username in usernames:
        log.info('Updating @%s' % username)
        update_account(mongo, username, load_extras=False)
        if quick:
            update_account_ops_quick(mongo, username)
        else:
            update_account_ops(mongo, username)
        indexer.set_checkpoint('accounts', username)
        log.info('Updated @%s' % username)

    # this was the last batch
    if account_checkpoint and len(usernames) < 1000:
        indexer.set_checkpoint('accounts', -1)


# Posts, Comments, Accounts, AccountOperations
# --------------------------------------------
def post_processing(mongo, batch_size=100, max_workers=50):
    indexer = Indexer(mongo)
    start_block = indexer.get_checkpoint('post_processing')

    query = {
        "block_num": {
            "$gt": start_block,
            "$lte": start_block + batch_size,
        }
    }
    projection = {
        '_id': 0,
        'body': 0,
        'json_metadata': 0,
    }
    results = list(mongo.Operations.find(query, projection=projection))
    batches = map(parse_operation, results)

    # handle an edge case when we are too close to the head,
    # and the batch contains no work to do
    if not results and is_recent(start_block, days=1):
        return

    # squash for duplicates
    def custom_merge(*args):
        return list(set(keep(flatten(args))))

    batch_items = merge_with(custom_merge, *batches)

    # upsert comments (recursively)
    with log_exceptions():
        list(thread_multi(
            fn=upsert_comment_chain,
            fn_args=[mongo, None],
            dep_args=batch_items['comments'],
            fn_kwargs=dict(recursive=True),
            max_workers=max_workers,
            yield_results=True,
        ))

    # only process accounts if the blocks are recent
    # scrape_all_users should take care of stale updates
    if is_recent(start_block, days=10):
        with log_exceptions():
            accounts = set(batch_items['accounts_light'] +
                           batch_items['accounts'])
            list(thread_multi(
                fn=update_account,
                fn_args=[mongo, None],
                dep_args=list(accounts),
                fn_kwargs=dict(load_extras=False),
                max_workers=max_workers,
                yield_results=True,
            ))
            list(thread_multi(
                fn=update_account_ops_quick,
                fn_args=[mongo, None],
                dep_args=list(accounts),
                fn_kwargs=None,
                max_workers=max_workers,
                yield_results=True,
            ))

    index = max(lpluck('block_num', results))
    indexer.set_checkpoint('post_processing', index)

    log.info("Checkpoint: %s - %s comments, %s accounts (+%s full)" % (
        index,
        len(batch_items['comments']),
        len(batch_items['accounts_light']),
        len(batch_items['accounts']),
    ))


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
    last_block = mongo.db['Blockchain'].find_one(
        filter={},
        projection={'_id': 0, 'block_num': 1},
        sort=[('block_num', -1)]
    )
    return last_block.get('block_num', 1) if last_block else 1


def is_recent(block_num, days):
    head_block_num = Steem().steemd.head_block_number
    return block_num > head_block_num - 20 * 60 * 24 * days


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
        # scrape_operations(m)
        scrape_comments(m)
        # post_processing(m)
        # update_account(m, 'furion', load_extras=True)
        # update_account_ops(m, 'furion')
        # scrape_all_users(m, False)
        # validate_operations(m)


if __name__ == '__main__':
    with suppress(KeyboardInterrupt):
        run()
