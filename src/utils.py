import os
from datetime import datetime

from funcy import contextmanager
from steem import Steem
from steemdata.helpers import simple_cache, create_cache
from steemdata.markets import Markets

usernames_cache = create_cache()

logger = None


def log_exception():
    """ Log to sentry.io. Alternatively,
    fallback to stdout stacktrace dump."""
    global logger

    dsn = os.getenv('SENTRY_DSN')
    if dsn:
        import raven
        logger = raven.Client(dsn)

    if logger:
        logger.captureException()
    else:
        import traceback
        print(traceback.format_exc())


@contextmanager
def log_exceptions():
    try:
        yield
    except:
        log_exception()


@simple_cache(usernames_cache, timeout=30 * 60)
def refresh_username_list():
    """
    Only refresh username list every 30 minutes, otherwise return from cache.
    """
    return get_all_usernames()


def get_all_usernames(last_user=-1, steem=None):
    if not steem:
        steem = Steem()

    usernames = steem.lookup_accounts(last_user, 1000)
    batch = []
    while len(batch) != 1:
        batch = steem.lookup_accounts(usernames[-1], 1000)
        usernames += batch[1:]

    return usernames


def get_usernames_batch(last_user=-1, steem=None):
    if not steem:
        steem = Steem()

    return steem.lookup_accounts(last_user, 1000)


def fetch_price_feed():
    m = Markets()
    return {
        "timestamp": datetime.utcnow(),
        "btc_usd": round(m.btc_usd(), 8),
        "steem_btc": round(m.steem_btc(), 8),
        "sbd_btc": round(m.sbd_btc(), 8),
        "steem_sbd_implied": round(m.steem_sbd_implied(), 6),
        "steem_usd_implied": round(m.steem_usd_implied(), 6),
        "sbd_usd_implied": round(m.sbd_usd_implied(), 6),
    }


def time_delta(item_time):
    delta = datetime.utcnow().replace(tzinfo=None) - item_time.replace(tzinfo=None)
    return delta.seconds


def strip_dot_from_keys(data: dict, replace_char='#') -> dict:
    """ Return a dictionary safe for MongoDB entry.

    ie. `{'foo.bar': 'baz'}` becomes `{'foo#bar': 'baz'}`
    """
    new_ = dict()
    for k, v in data.items():
        if type(v) == dict:
            v = strip_dot_from_keys(v)
        if '.' in k:
            k = k.replace('.', replace_char)
        new_[k] = v
    return new_
