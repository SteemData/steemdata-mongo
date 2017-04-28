import inspect
import os

from celery import Celery

from methods import update_account, update_account_ops_quick, upsert_comment_chain
from mongostorage import MongoStorage, DB_NAME, MONGO_HOST, MONGO_PORT
from utils import log_exceptions

# override a node for perf reasons
_custom_node = False


def new_celery(worker_name: str):
    return Celery(worker_name,
                  backend=os.getenv('CELERY_BACKEND_URL', 'redis://localhost:6379/0'),
                  broker=os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/0'))


def caller_name(skip=7):
    """Get a name of a caller in the format module.class.method

       `skip` specifies how many levels of stack to skip while getting caller
       name. skip=1 means "who calls me", skip=2 "who calls my caller" etc.

       An empty string is returned if skipped levels exceed stack height
    """
    stack = inspect.stack()
    start = 0 + skip
    if len(stack) < start + 1:
        return ''
    parentframe = stack[start][0]

    name = []
    module = inspect.getmodule(parentframe)
    # `modname` can be None when frame is executed directly in console
    # TODO(techtonik): consider using __main__
    if module:
        name.append(module.__name__)
    # detect classname
    if 'self' in parentframe.f_locals:
        # I don't know any way to detect call from the object method
        # XXX: there seems to be no way to detect static method call - it will
        #      be just a function call
        name.append(parentframe.f_locals['self'].__class__.__name__)
    codename = parentframe.f_code.co_name
    if codename != '<module>':  # top level usually
        name.append(codename)  # function or a method
    del parentframe
    return ".".join(name)


def override_steemd():
    """override steemd node list for this worker"""
    from steem.steemd import Steemd
    from steem.instance import set_shared_steemd_instance

    global _custom_node

    if not _custom_node:
        steemd_nodes = [
            'https://gtg.steem.house:8090',
            'https://steemd.steemit.com',
        ]
        set_shared_steemd_instance(Steemd(nodes=steemd_nodes))
        _custom_node = True


def ensure_eu_node():
    from steem.instance import shared_steemd_instance

    if _custom_node:
        instance = shared_steemd_instance()
        if instance.hostname != 'https://gtg.steem.house:8090':
            instance.set_node('https://gtg.steem.house:8090')


# only run this code from celery worker
# we don't want to do global overrides for other processes
if str(caller_name()) != '__main__':
    mongo = MongoStorage(db_name=os.getenv('DB_NAME', DB_NAME),
                         host=os.getenv('DB_HOST', MONGO_HOST),
                         port=os.getenv('DB_PORT', MONGO_PORT))
    mongo.ensure_indexes()

    override_steemd()

# task definitions
# ----------------
tasks = new_celery('tasks')


@tasks.task
def update_account_async(account_name, load_extras=False):
    update_account(mongo, account_name, load_extras=load_extras)
    update_account_ops_quick(mongo, account_name)


@tasks.task
def update_comment_async(post_identifier, recursive=False):
    upsert_comment_chain(mongo, post_identifier, recursive)


@tasks.task
def batch_update_async(batch_items: dict):
    # try to always be on the EU node
    ensure_eu_node()
    for account_name in batch_items['accounts']:
        with log_exceptions():
            update_account(mongo, account_name, load_extras=True)
            update_account_ops_quick(mongo, account_name)
    for account_name in batch_items['accounts_light']:
        with log_exceptions():
            update_account(mongo, account_name, load_extras=False)
            update_account_ops_quick(mongo, account_name)
    for identifier in batch_items['comments']:
        with log_exceptions():
            upsert_comment_chain(mongo, identifier, recursive=True)
