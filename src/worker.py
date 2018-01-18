import multiprocessing
import os
import sys
import time
from contextlib import suppress
from multiprocessing.pool import Pool

from mongostorage import (
    MongoStorage,
    DB_NAME,
    MONGO_HOST,
    MONGO_PORT,
)
from scraper import (
    scrape_all_users,
    scrape_operations,
    scrape_prices,
    refresh_dbstats,
    scrape_comments,
    post_processing,
)
from utils import log_exception


def run(worker_name):
    mongo = MongoStorage(
        db_name=os.getenv('DB_NAME', DB_NAME),
        host=os.getenv('DB_HOST', MONGO_HOST),
        port=os.getenv('DB_PORT', MONGO_PORT))

    while True:
        try:
            if worker_name == 'scrape_operations':
                mongo.ensure_indexes()
                scrape_operations(mongo)
            elif worker_name == 'scrape_comments':
                scrape_comments(mongo)
            elif worker_name == 'post_processing':
                post_processing(mongo)
            elif worker_name == 'scrape_all_users':
                scrape_all_users(mongo, quick=False)
            elif worker_name == 'scrape_prices':
                scrape_prices(mongo)
            elif worker_name == 'refresh_dbstats':
                refresh_dbstats(mongo)
            else:
                print(f'Worker "{worker_name}" does not exist!')
                quit(1)
        except (KeyboardInterrupt, SystemExit):
            print('Quitting...')
            exit(0)
        except Exception as e:
            print('Exception in worker:', worker_name)
            log_exception()
            time.sleep(5)

        # prevent IO overflow
        time.sleep(0.5)


def run_multi():
    multiprocessing.set_start_method('spawn')
    workers = [
        'scrape_all_users',
        'scrape_operations',
    ]

    with Pool(len(workers)) as p:
        p.map(run, workers)


def main():
    with suppress(KeyboardInterrupt):
        try:
            _, worker_name = sys.argv
            print("Starting worker: '%s'" % worker_name)
            run(worker_name)
        except ValueError:
            print('Usage: python workers.py <worker_name>')


if __name__ == "__main__":
    main()
    # run_multi()
    # run('scrape_operations')
