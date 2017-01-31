import time
from contextlib import contextmanager
from datetime import datetime

from funcy import flatten
from steem import Steem
from steemdata.markets import Markets


def fetch_comments_flat(root_post=None, comments=list(), all_comments=list()):
    """
    Recursively fetch all the child comments, and return them as a list.

    Usage: all_comments = fetch_comments_flat(Post('@foo/bar'))
    """
    # see if our root post has any comments
    if root_post:
        return fetch_comments_flat(comments=root_post.get_comments())
    if not comments:
        return all_comments

    # recursively scrape children one depth layer at a time
    children = list(flatten([x.get_comments() for x in comments]))
    if not children:
        return all_comments
    return fetch_comments_flat(all_comments=comments + children, comments=children)


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


@contextmanager
def timeit():
    t1 = time.time()
    yield
    print("Time Elapsed: %.2f" % (time.time() - t1))


def fetch_price_feed():
    m = Markets()
    return {
        "timestamp": datetime.utcnow(),
        "btc_usd": float("%.4f" % m.btc_usd()),
        "steem_btc": float("%.8f" % m.steem_btc()),
        "sbd_btc": float("%.8f" % m.sbd_btc()),
        "steem_sbd_implied": float("%.4f" % m.steem_sbd_implied()),
        "steem_usd_implied": float("%.4f" % m.steem_usd_implied()),
        "sbd_usd_implied": float("%.4f" % m.sbd_usd_implied()),
    }
