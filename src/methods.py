import datetime as dt
import json
from contextlib import suppress

import pymongo
from funcy import compose, take, first, second
from pymongo.errors import DuplicateKeyError, WriteError
from steem.account import Account
from steem.post import Post
from steem.utils import keep_in_dict
from steembase.exceptions import PostDoesNotExist
from steemdata.utils import typify, json_expand, remove_body


def parse_operation(op):
    """ Update all relevant collections that this op impacts. """
    op_type = op['type']

    update_accounts_light = set()
    update_accounts_full = set()
    update_comments = set()

    def construct_identifier():
        return '@%s/%s' % (
            op.get('author', op.get('comment_author')),
            op.get('permlink', op.get('comment_permlink')),
        )

    def account_from_auths():
        return first(op.get('required_auths', op.get('required_posting_auths')))

    if op_type in ['account_create',
                   'account_create_with_delegation']:
        update_accounts_light.add(op['creator'])
        update_accounts_full.add(op['new_account_name'])

    elif op_type in ['account_update',
                     'withdraw_vesting',
                     'claim_reward_balance',
                     'return_vesting_delegation',
                     'account_witness_vote']:
        update_accounts_light.add(op['account'])

    elif op_type == 'account_witness_proxy':
        update_accounts_light.add(op['account'])
        update_accounts_light.add(op['proxy'])

    elif op_type in ['author_reward', 'comment']:
        update_accounts_light.add(op['author'])
        update_comments.add(construct_identifier())

    elif op_type == 'cancel_transfer_from_savings':
        update_accounts_light.add(op['from'])

    elif op_type == 'change_recovery_account':
        update_accounts_light.add(op['account_to_recover'])

    elif op_type == 'comment_benefactor_reward':
        update_accounts_light.add(op['benefactor'])

    elif op_type == ['convert',
                     'fill_convert_request',
                     'interest',
                     'limit_order_cancel',
                     'limit_order_create',
                     'shutdown_witness',
                     'witness_update']:
        update_accounts_light.add(op['owner'])

    elif op_type == 'curation_reward':
        update_accounts_light.add(op['curator'])

    elif op_type in ['custom', 'custom_json']:
        update_accounts_light.add(account_from_auths())

    elif op_type == 'delegate_vesting_shares':
        update_accounts_light.add(op['delegator'])
        update_accounts_light.add(op['delegatee'])

    elif op_type == 'delete_comment':
        update_accounts_light.add(op['author'])

    elif op_type in ['escrow_approve',
                     'escrow_dispute',
                     'escrow_release',
                     'escrow_transfer']:
        accs = keep_in_dict(op, ['agent', 'from', 'to', 'who', 'receiver']).values()
        update_accounts_light.update(accs)

    elif op_type == 'feed_publish':
        update_accounts_light.add(op['publisher'])

    elif op_type in ['fill_order']:
        update_accounts_light.add(op['open_owner'])
        update_accounts_light.add(op['current_owner'])

    elif op_type in ['fill_vesting_withdraw']:
        update_accounts_light.add(op['to_account'])
        update_accounts_light.add(op['from_account'])

    elif op_type == 'pow2':
        acc = op['work'][1]['input']['worker_account']
        update_accounts_light.add(acc)

    elif op_type in ['recover_account',
                     'request_account_recovery']:
        update_accounts_light.add(op['account_to_recover'])

    elif op_type == 'set_withdraw_vesting_route':
        update_accounts_light.add(op['from_account'])
        # update_accounts_light.add(op['to_account'])
    elif op_type in ['transfer',
                     'transfer_from_savings',
                     'transfer_to_savings',
                     'transfer_to_vesting']:
        accs = keep_in_dict(op, ['agent', 'from', 'to', 'who', 'receiver']).values()
        update_accounts_light.update(accs)

    elif op_type == 'vote':
        update_accounts_light.add(op['voter'])
        update_comments.add(construct_identifier())

    # handle followers
    if op_type == 'custom_json':
        with suppress(ValueError):
            cmd, op_json = json.loads(op['json'])  # ['follow', {data...}]
            if cmd == 'follow':
                accs = keep_in_dict(op_json, ['follower', 'following']).values()
                update_accounts_light.discard(first(accs))
                update_accounts_light.discard(second(accs))
                update_accounts_full.update(accs)

    return {
        'accounts': list(update_accounts_full),
        'accounts_light': list(update_accounts_light),
        'comments': list(update_comments),
    }


def upsert_comment_chain(mongo, identifier, recursive=False):
    """ Upsert given comments and its parent(s).
    
    Args:
        mongo: mongodb instance
        identifier: Post identifier
        recursive: (Defaults to False). If True, recursively update all parent comments, incl. root post.
    """
    with suppress(PostDoesNotExist):
        p = Post(identifier)
        if p.is_comment():
            mongo.Comments.update({'identifier': p.identifier}, p.export(), upsert=True)
            parent_identifier = '@%s/%s' % (p.parent_author, p.parent_permlink)
            if recursive:
                upsert_comment_chain(mongo, parent_identifier, recursive)
            else:
                upsert_comment(mongo, parent_identifier)
        else:
            return mongo.Posts.update({'identifier': p.identifier}, p.export(), upsert=True)


def upsert_comment(mongo, identifier):
    """ Upsert root post or comment. """
    with suppress(PostDoesNotExist):
        p = Post(identifier)
        if p.is_comment():
            return mongo.Comments.update({'identifier': p.identifier}, p.export(), upsert=True)
        return mongo.Posts.update({'identifier': p.identifier}, p.export(), upsert=True)


def delete_comment(mongo, identifier):
    return mongo.Comments.update({'identifier': identifier}, {'$set': {'is_deleted': True}}, upsert=True)


def update_account(mongo, username, load_extras=True):
    """ Update Account. 
    
    If load_extras is True, update:
     - followers, followings
     - curation stats
     - withdrawal routers, conversion requests

    """
    a = Account(username)
    account = {
        **typify(a.export(load_extras=load_extras)),
        'account': username,
        'updatedAt': dt.datetime.utcnow(),
    }
    if not load_extras:
        account = {'$set': account}
    try:
        mongo.Accounts.update({'name': a.name}, account, upsert=True)
    except WriteError:
        # likely an invalid profile
        account['json_metadata'] = {}
        mongo.Accounts.update({'name': a.name}, account, upsert=True)
        print("Invalidated json_metadata on %s" % a.name)


def update_account_ops(mongo, username):
    """ This method will fetch entire account history, and back-fill any missing ops. """
    for event in Account(username).history():
        with suppress(DuplicateKeyError):
            transform = compose(remove_body, json_expand, typify)
            mongo.AccountOperations.insert_one(transform(event))


def account_operations_index(mongo, username):
    """ Lookup AccountOperations for latest synced index. """
    start_index = 0
    # use projection to ensure covered query
    highest_index = list(mongo.AccountOperations.find({'account': username}, {'_id': 0, 'index': 1}).
                         sort("index", pymongo.DESCENDING).limit(1))
    if highest_index:
        start_index = highest_index[0].get('index', 0)

    return start_index


def update_account_ops_quick(mongo, username, batch_size=200, steemd_instance=None):
    """ Only update the latest missing history, limited to 1 batch of defined batch_size. """
    start_index = account_operations_index(mongo, username)

    # fetch latest records and update the db
    history = Account(username, steemd_instance=steemd_instance).history_reverse(batch_size=batch_size)
    for event in take(batch_size, history):
        if event['index'] < start_index:
            return
        with suppress(DuplicateKeyError):
            mongo.AccountOperations.insert_one(json_expand(typify(event)))


def find_latest_item(mongo, collection_name, field_name):
    last_op = mongo.db[collection_name].find_one(
        filter={},
        projection={field_name: 1, '_id': 0},
        sort=[(field_name, pymongo.DESCENDING)],
    )
    return last_op[field_name]

# def _get_ops(block_nums):
#     s = Steem().steemd
#     params = zip(block_nums, repeat(False))
#     results = s.exec_multi_with_futures('get_ops_in_block', params, False, max_workers=10)
#     return list(results)
#
#
# def get_ops(block_nums: List[int]):
#     operations = {}
#     results = []
#
#     while not results:
#         results = _get_ops(block_nums)
#     # todo: sort by block num
#     results = [x for x in results if x]
#     return results
#
#
# def get_ops_range(start: int, end: int):
#     return get_ops(list(range(start, end)))
#
#
# if __name__ == '__main__':
#     for batch in partition_all(100, range(2000000, 4010000)):
#         print(batch[0], batch[-1])
#         results = _get_ops(batch)
#         negatives = [x for x in results if not x]
#         print(len(negatives))
#         # print(get_ops(batch))
