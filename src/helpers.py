import time
from contextlib import contextmanager
from datetime import datetime

from steemdata.markets import Markets


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
