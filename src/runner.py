import multiprocessing
import traceback
from multiprocessing.pool import Pool

from steem import Steem

from mongostorage import MongoStorage
from scraper import scrape_all_users, scrape_operations, scrape_virtual_operations, scrape_active_posts, scrape_misc


def run_worker(worker_name):
    while True:
        # init
        mongo = MongoStorage()
        mongo.ensure_indexes()
        stm = Steem()

        try:
            if worker_name == "scrape_all_users":
                scrape_all_users(mongo, stm)
            elif worker_name == "scrape_operations":
                scrape_operations(mongo, stm)
            elif worker_name == "scrape_virtual_operations":
                scrape_virtual_operations(mongo, stm)
            elif worker_name == "scrape_active_posts":
                scrape_active_posts(mongo, stm)
            elif worker_name == "scrape_misc":
                scrape_misc(mongo)
        except (KeyboardInterrupt, SystemExit):
            exit("Quitting...")
        except:
            print("EXCEPTION: %s():" % worker_name)
            print(traceback.format_exc())


if __name__ == '__main__':
    multiprocessing.set_start_method('spawn')
    workers = [
        'scrape_all_users',
        'scrape_operations',
        'scrape_virtual_operations',
        'scrape_active_posts',
        'scrape_misc',
    ]

    with Pool(len(workers)) as p:
        print(p.map(run_worker, workers))
