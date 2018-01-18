#!/usr/bin/python
# -*- coding: utf-8 -*-

import pymongo
from pymongo.errors import ConnectionFailure

MONGO_HOST = 'localhost'
MONGO_PORT = 27017
DB_NAME = 'SteemData'


class MongoStorage(object):
    def __init__(self, db_name=DB_NAME, host=MONGO_HOST, port=MONGO_PORT):
        try:
            mongo_url = 'mongodb://%s:%s/%s' % (host, port, db_name)
            client = pymongo.MongoClient(mongo_url)
            self.db = client[db_name]

        except ConnectionFailure as e:
            print('Can not connect to MongoDB server: %s' % e)
            raise
        else:
            self.Blockchain = self.db['Blockchain']
            self.Accounts = self.db['Accounts']
            self.Posts = self.db['Posts']
            self.Comments = self.db['Comments']
            self.Operations = self.db['Operations']
            self.AccountOperations = self.db['AccountOperations']
            self.PriceHistory = self.db['PriceHistory']

    def list_collections(self):
        return self.db.collection_names()

    def reset_db(self):
        for col in self.list_collections():
            self.db.drop_collection(col)

    def ensure_indexes(self):
        self.Blockchain.create_index('previous', unique=True)
        self.Blockchain.create_index('block_id', unique=True)
        self.Blockchain.create_index([('block_num', -1)])

        self.Accounts.create_index('name', unique=True)

        # Operations are using _id as unique index
        self.Operations.create_index([('type', 1), ('timestamp', -1)])
        self.Operations.create_index([('block_id', 1)])
        self.Operations.create_index([('type', 1)])
        self.Operations.create_index([('block_num', -1)])
        self.Operations.create_index([('timestamp', -1)])
        # partial indexes
        self.Operations.create_index([('author', 1), ('permlink', 1)], sparse=True, background=True)
        self.Operations.create_index([('to', 1)], sparse=True, background=True)
        self.Operations.create_index([('from', 1)], sparse=True, background=True)
        self.Operations.create_index([('memo', pymongo.HASHED)], sparse=True, background=True)

        # AccountOperations are using _id as unique index
        self.AccountOperations.create_index([('account', 1), ('type', 1), ('timestamp', -1)])
        self.AccountOperations.create_index([('account', 1), ('type', 1)])
        self.AccountOperations.create_index([('account', 1)])
        self.AccountOperations.create_index([('type', 1)])
        self.AccountOperations.create_index([('timestamp', -1)])
        self.AccountOperations.create_index([('index', -1)])

        self.Posts.create_index([('author', 1), ('permlink', 1)], unique=True)
        self.Posts.create_index([('identifier', 1)], unique=True)
        self.Posts.create_index([('author', 1)])
        self.Posts.create_index([('created', -1)])
        self.Posts.create_index([('json_metadata.app', 1)], background=True, sparse=True)
        self.Posts.create_index([('json_metadata.users', 1)], background=True, sparse=True)
        self.Posts.create_index([('json_metadata.tags', 1)], background=True, sparse=True)
        self.Posts.create_index([('json_metadata.community', 1)], background=True, sparse=True)
        self.Posts.create_index([('body', 'text'), ('title', 'text')], background=True)

        self.Comments.create_index([('identifier', 1)], unique=True)
        self.Comments.create_index([('parent_author', 1)])
        self.Comments.create_index([('parent_permlink', 1)])
        self.Comments.create_index([('author', 1)])
        self.Comments.create_index([('permlink', 1)])
        self.Comments.create_index([('created', -1)])
        self.Comments.create_index([('body', 'text'), ('title', 'text')], background=True)

        self.PriceHistory.create_index([('timestamp', -1)])

        # 4 jesta's tools
        self.Operations.create_index(
            [('producer', 1), ('type', 1), ('timestamp', 1)],
            sparse=True, background=True)
        self.Operations.create_index(
            [('curator', 1), ('type', 1), ('timestamp', 1)],
            sparse=True, background=True)
        self.Operations.create_index(
            [('benefactor', 1), ('type', 1), ('timestamp', 1)],
            sparse=True, background=True)
        self.Operations.create_index(
            [('author', 1), ('type', 1), ('timestamp', 1)],
            sparse=True, background=True)


class Indexer(object):
    def __init__(self, mongo):
        self.coll = mongo.db['_indexer']
        self.instance = self.coll.find_one()

        if not self.instance:
            self.coll.insert_one({
                "operations_checkpoint": 1,
            })
            self.instance = self.coll.find_one()

    def get_checkpoint(self, name):
        field = f'{name}_checkpoint'
        return self.instance.get(field, 1)

    def set_checkpoint(self, name, index):
        field = f'{name}_checkpoint'
        return self.coll.update_one({}, {"$set": {field: index}})


class Stats(object):
    def __init__(self, mongo):
        self.mongo = mongo
        self._stats = mongo.db['stats']
        self.stats = self._stats.find_one()

    def refresh(self):
        return self._stats.update({}, self._compile_stats(), upsert=True)

    def _compile_stats(self):
        # pprint(self.mongo.db.command('collstats', 'Accounts'))
        return {
            **{k: {
                'count': self.mongo.db[k].find().count(),
                'size': self.mongo.db.command('collstats', k).get('storageSize', 1) / 1e6,
            }
               for k in self.mongo.list_collections()},
            'dbSize': self.mongo.db.command('dbstats', 1000).get('storageSize', 1) / 1e6
        }


if __name__ == '__main__':
    mongo = MongoStorage()
    Stats(mongo).refresh()
