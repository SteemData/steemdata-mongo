import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import List, Any, Union

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


# ---------------
# Multi-Threading
# ---------------
def ensure_list(parameter):
    return parameter if type(parameter) in (list, tuple, set) else [parameter]


def dependency_injection(fn_args, dep_args):
    """
    >>> dependency_injection([1, None, None], [2,3])
    [1, 2, 3]
    """
    fn_args = ensure_list(fn_args)
    dep_args = ensure_list(dep_args)[::-1]

    args = []
    for fn_arg in fn_args:
        next_arg = fn_arg if fn_arg is not None else dep_args.pop()
        args.append(next_arg)

    return args


def thread_multi(
        fn,
        fn_args: List[Any],
        dep_args: List[Union[Any, List[Any]]],
        fn_kwargs=None,
        max_workers=100,
        yield_results=False):
    """ Run a function /w variable inputs concurrently.

    Args:
        fn: A pointer to the function that will be executed in parallel.
        fn_args: A list of arguments the function takes. None arguments will be
        displaced trough `dep_args`.
        dep_args: A list of lists of arguments to displace in `fn_args`.
        fn_kwargs: Keyword arguments that `fn` takes.
        max_workers: A cap of threads to run in parallel.
        yield_results: Yield or discard results.
    """
    if not fn_kwargs:
        fn_kwargs = dict()

    fn_args = ensure_list(fn_args)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = (executor.submit(fn, *dependency_injection(fn_args, args), **fn_kwargs)
                   for args in dep_args)

        if yield_results:
            for result in as_completed(futures):
                yield result.result()
        else:
            for _ in as_completed(futures):
                continue
