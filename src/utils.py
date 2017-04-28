import traceback
from datetime import datetime

from funcy.decorators import contextmanager
from steem import Steem
from steemdata.helpers import simple_cache, create_cache
from steemdata.markets import Markets

usernames_cache = create_cache()


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


@contextmanager
def log_exceptions():
    try:
        yield
    except:
        print(traceback.format_exc())
