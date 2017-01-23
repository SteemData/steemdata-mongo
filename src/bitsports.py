from scraper import update_account_ops, update_account


def refresh_bitsports(mongo, steem):
    for account in ['steemsports', 'chessmasters', 'steemgames']:
        update_account(mongo, steem, account)
        update_account_ops(mongo, steem, account)


# if __name__ == "__main__":
#     mongo = MongoStorage()
#     mongo.ensure_indexes()
#     stm = Steem(node='ws://51.15.54.34:8090')
#     refresh_bitsports(mongo, stm)
