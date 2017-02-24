import multiprocessing
import os
import time
import traceback
from multiprocessing.pool import Pool

from steem import Steem

from mongostorage import MongoStorage, MONGO_HOST, DB_NAME, MONGO_PORT
from scraper import (
    scrape_all_users,
    scrape_operations,
    scrape_active_posts,
    scrape_prices,
    override,
    refresh_dbstats,
    validate_operations,
)


def run_worker(worker_name):
    mongo = MongoStorage(db_name=os.getenv('DB_NAME', DB_NAME),
                         host=os.getenv('DB_HOST', MONGO_HOST),
                         port=os.getenv('DB_PORT', MONGO_PORT))
    stm = Steem()

    while True:
        try:
            if worker_name == "scrape_operations":
                mongo.ensure_indexes()
                scrape_operations(mongo, stm)
            elif worker_name == "validate_operations":
                validate_operations(mongo, stm)
            elif worker_name == "scrape_all_users":
                scrape_all_users(mongo, stm)
            elif worker_name == "scrape_active_posts":
                scrape_active_posts(mongo, stm)
            elif worker_name == "scrape_prices":
                scrape_prices(mongo)
            elif worker_name == "refresh_dbstats":
                refresh_dbstats(mongo)
            elif worker_name == "override":
                override(mongo)
        except (KeyboardInterrupt, SystemExit):
            exit("Quitting...")
        except:
            print("EXCEPTION: %s():" % worker_name)
            print(traceback.format_exc())

        # prevent IO overflow
        time.sleep(5)


if __name__ == '__main__':
    multiprocessing.set_start_method('spawn')
    workers = [
        'scrape_all_users',
        'scrape_operations',
        'scrape_active_posts',
        'scrape_prices',
        # 'scrape_misc',
    ]

    with Pool(len(workers)) as p:
        p.map(run_worker, workers)
